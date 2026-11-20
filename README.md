# Parquet footer 统计驱动的保守 plan/scan

用 Python 3.11 + PyArrow 19 实现的小型 Parquet 查询工具：计划阶段只读 footer 统计、
保守剪枝行组；执行阶段先读过滤列做 SQL 三值逻辑精确过滤，再只为命中行读投影列。
不解析 SQL、不做损坏恢复、不使用 page index、不含远程 Range 服务与性能压测。

## 目录

```
fixtures/                 输入：files.json、queries.json、stats.parquet、no-stats.parquet
tools/make_fixtures.py    从同一份数据生成“有统计/无统计”两个 Parquet（行组与行序一致）
src/pqscan/
  expr.py       谓词树解析、schema 校验、PyArrow 表达式翻译、纯 Python 3VL 逐行求值
  stats.py      只读 footer 的行组统计提取与可靠性判定
  planner.py    三值逻辑保守行组推理（can_be_true / can_be_false、trace）
  scanner.py    两阶段列读取、精确行选择、读取请求在执行入口即时登记
  reference.py  独立核对：无剪枝全扫描 + 纯 Python 3VL（与引擎不同代码路径）
  runner.py     串联计划/扫描/核对，落盘 plans、results、run_record、verification
  cli.py        命令行入口
tests/          pytest 套件（12 个用例）
docs/plan_format.md  计划与运行记录的 JSON 格式契约
output/         运行产物（git 忽略）
```

## 运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python tools/make_fixtures.py           # 重新生成两个夹具（可选，仓库已带）
PYTHONPATH=src .venv/bin/python -m pqscan.cli run \
    --files fixtures/files.json --queries fixtures/queries.json --out output
PYTHONPATH=src .venv/bin/python -m pytest tests/ -q
```

输出：`output/plans/*.json`（行组选择说明）、`output/results/*.parquet`（查询结果）、
`output/run_record.json`（实际列读取请求）、`output/verification.json`（独立核对结论）。

## 支持范围

- 平面 schema；整数（有/无符号 int32/int64 等）与字符串列；
- 比较 `eq/ne/lt/le/gt/ge`、`is_null`、`and/or/not`、列投影；
- 谓词以 JSON 前缀树表达（见 `fixtures/queries.json`），不解析 SQL；
- 类型不匹配、列不存在、嵌套 schema 等在计划期报错；不处理文件损坏。

## 关键设计约束（与需求逐条对应）

1. **计划只读 footer。** `stats.py` 只访问 `FileMetaData/RowGroupMetaData/Statistics`，
   不调用任何数据读取接口。
2. **三值逻辑双结论。** 每个谓词子树求 `can_be_true`/`can_be_false`，
   NULL 产生的 UNKNOWN 既不算 TRUE 也不算 FALSE。`NOT` 同时翻转两个结论，
   不会出现“把 UNKNOWN 当 FALSE 再取反”的错误剪枝（见 `planner.py`）。
3. **统计缺失/不可靠即保守保留。** 无 Statistics、`has_min_max=False`（全 NULL 组的
   典型形态）、`null_count` 缺失、物理类型不受支持，叶子结论退化为
   `(true, true)` 并在 trace 的 `unknown` 中登记原因。PyArrow 19 的 Statistics
   不暴露 min/max 截断标记，因此对存在且类型受支持的 min/max 视为精确；
   若环境提供截断信息，应在 `stats.py` 将其降级为不可靠。
4. **AND/OR 不跨列关联极值。** 只组合独立子树的两个结论，不假设不同列的
   min/max 来自同一行。只有证明 `can_be_true=false` 才 `skip`。
5. **两阶段扫描。** 保留组先读过滤列；`Table.filter` 的 Kleene 语义丢弃
   UNKNOWN/FALSE、只留 TRUE，再用临时行号列取回组内物理行号。无命中组到此结束，
   不读 payload 等投影专用列；有命中组才读剩余投影列。过滤列同时在投影中时复用
   第一阶段数组，不会发起第二次读取（空列集合不产生请求）。
6. **读取计数来自执行入口。** `InstrumentedParquetFile.read_row_group` 在真实
   调用发生时登记 `(文件, 行组, phase, 列集合)`，计划器不参与填写；记录的是工具
   发起的列读取请求，不是操作系统字节数，也不用 footer 的压缩大小冒充 I/O。
7. **结果与溯源。** 输出按 files.json 文件顺序、行组物理索引、组内行号排列；
   附 `_source_file/_source_row_group/_source_row_in_group` 三列；重复数据保留；
   空结果写出同 schema 的零行 Parquet。
8. **独立核对。** `reference.py` 不看任何统计、每个行组全列全量读取，用纯 Python
   3VL 逐行求值，逐行比对引擎结果的内容、顺序与溯源。

## 夹具与现成查询的预期现象

两个 Parquet 由同一份 20 行数据写成，各 5 个行组、每组 4 行；RG2 的 `k` 全 NULL。

| 查询 | stats.parquet 计划 | no-stats.parquet 计划 |
| --- | --- | --- |
| `range`（k∈[14,30)） | 跳过 RG0/RG1/RG4；RG2 全 NULL 保守保留但过滤后无命中；仅 RG3 命中 | 5 组全保留，结果完全一致 |
| `nulls`（k IS NULL） | null_count=0 的 RG1/RG3/RG4 证明跳过；RG0/RG2 命中 | 全保留，结果一致 |
| `negated`（NOT kind='A'） | 全部保留（含 NULL 行，UNKNOWN 不取反为 TRUE） | 全保留，结果一致 |
| `or_missing_stats`（kind='A' OR amount>160） | 全部保留（OR 跨列独立推理） | 全保留，结果一致 |
| `no_hits`（k>1000） | RG0/RG1/RG3/RG4 证明跳过；RG2 统计缺失保守保留、过滤后无命中 | 全保留、全部无命中 |

可在 `output/run_record.json` 验证：`no_hits` 与 `range` 中所有
`no_hits_after_filter` 行组只有 `phase=filter` 的请求，从不读取投影专用列
（`row_id/payload/kind/amount`）；`planned_skip` 行组零请求。

## 测试覆盖

`tests/test_pqscan.py`（12 用例）覆盖：范围剪枝、全 NULL 组、IS NULL 的
null_count 推理、NOT 的 UNKNOWN 处理与可证明 FALSE、OR 跨列不关联极值、
无统计全保留、3VL 真值表、无命中不读投影列、过滤列复用、端到端与无剪枝
全扫描一致、有/无统计结果相同而计划保守度不同、空结果 schema 与输出顺序。
