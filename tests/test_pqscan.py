"""pytest 覆盖：footer 剪枝正确性、3VL 行级语义、两阶段列读取、独立全扫描核对。

运行：PYTHONPATH=src .venv/bin/python -m pytest tests/ -q
"""
from __future__ import annotations

import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pqscan.expr import eval_row, parse_queries, predicate_columns, validate
from pqscan.planner import plan_file
from pqscan.runner import run
from pqscan.scanner import InstrumentedParquetFile, scan_query
from pqscan.stats import read_footer_stats

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("a", pa.int64()),
        pa.field("b", pa.string()),
        pa.field("payload", pa.string()),
    ]
)

GROUPS = [
    # RG0: 普通组，a 有正有负，b 有 NULL
    {"id": [0, 1], "a": [-5, 5], "b": [None, "X"], "payload": ["p0", "p1"]},
    # RG1: a 全 NULL
    {"id": [2, 3], "a": [None, None], "b": ["Y", "Z"], "payload": ["p2", "p3"]},
    # RG2: a 全部很大
    {"id": [4, 5], "a": [100, 200], "b": ["X", "W"], "payload": ["p4", "p5"]},
]


@pytest.fixture(scope="module")
def parquet_pair(tmp_path_factory):
    base = tmp_path_factory.mktemp("pq")
    stats_path = str(base / "with_stats.parquet")
    nostats_path = str(base / "no_stats.parquet")
    tables = [pa.table(g, schema=SCHEMA) for g in GROUPS]

    w = pq.ParquetWriter(stats_path, SCHEMA, write_statistics=True)
    for t in tables:
        w.write_table(t)
    w.close()

    w = pq.ParquetWriter(nostats_path, SCHEMA, write_statistics=False)
    for t in tables:
        w.write_table(t)
    w.close()
    return stats_path, nostats_path


def _decisions(path, predicate):
    schema = pq.ParquetFile(path).schema_arrow
    validate(predicate, schema)
    groups = read_footer_stats(path, schema)
    return plan_file(path, predicate, groups)


# ---------------------------------------------------------------------------
# 计划阶段：footer 统计推理
# ---------------------------------------------------------------------------

def test_range_prunes_provable_groups(parquet_pair):
    stats_path, _ = parquet_pair
    pred = {"and": [{"ge": ["a", 0]}, {"lt": ["a", 50]}]}
    actions = {d.row_group: d.action for d in _decisions(stats_path, pred)}
    assert actions == {0: "keep", 1: "keep", 2: "skip"}
    # RG1 全 NULL：min/max 缺失但 null_count 可靠，结论保守保留
    rg1 = next(d for d in _decisions(stats_path, pred) if d.row_group == 1)
    assert rg1.missing_stats == ("a",)
    assert rg1.can_be_true and rg1.can_be_false


def test_is_null_uses_null_count(parquet_pair):
    stats_path, _ = parquet_pair
    actions = {d.row_group: d.action for d in _decisions(stats_path, {"is_null": "a"})}
    # RG0/RG2 的 null_count=0 可证明跳过；RG1 全 NULL 保留
    assert actions == {0: "skip", 1: "keep", 2: "skip"}


def test_negation_does_not_treat_unknown_as_false(parquet_pair):
    """NOT 在统计缺失时必须保守保留：UNKNOWN 取反还是 UNKNOWN，不是 TRUE。"""
    _, nostats_path = parquet_pair
    decisions = _decisions(nostats_path, {"not": {"eq": ["b", "X"]}})
    assert all(d.action == "keep" for d in decisions)
    assert all(d.can_be_true and d.can_be_false for d in decisions)


def test_not_of_provable_false(parquet_pair):
    """有统计时 NOT(a > 50)：
    - RG0(a=-5,5)：内部 a>50 不可能 TRUE -> NOT 不可能 FALSE/UNKNOWN 以外，
      即 NOT 恒 TRUE，can_be_false=False（跳过它反而不合法，因为确有 TRUE 行，故保留）；
    - RG1 全 NULL：NOT(UNKNOWN)=UNKNOWN，两个结论都保守为真；
    - RG2(a=100,200)：内部恒 TRUE -> NOT 恒 FALSE，可证明跳过。"""
    stats_path, _ = parquet_pair
    decisions = {d.row_group: d for d in _decisions(stats_path, {"not": {"gt": ["a", 50]}})}
    assert decisions[0].action == "keep" and decisions[0].can_be_false is False
    assert decisions[1].action == "keep"
    assert decisions[1].can_be_true and decisions[1].can_be_false
    assert decisions[2].action == "skip"


def test_or_across_columns_never_correlates_extremes(parquet_pair):
    """OR(b='X', a>150)：不假设同列/同行极值，仅按独立子树结论组合。
    RG1 的 a 统计缺失 -> 整棵 OR 保守保留；RG0 b 可能为 X -> 保留。"""
    stats_path, _ = parquet_pair
    pred = {"or": [{"eq": ["b", "X"]}, {"gt": ["a", 150]}]}
    decisions = {d.row_group: d for d in _decisions(stats_path, pred)}
    assert all(d.action == "keep" for d in decisions.values())
    assert decisions[1].missing_stats == ("a",)


def test_missing_stats_keeps_everything(parquet_pair):
    _, nostats_path = parquet_pair
    decisions = _decisions(nostats_path, {"gt": ["a", 10_000]})
    assert all(d.action == "keep" for d in decisions)
    assert all(d.missing_stats == ("a",) for d in decisions)


# ---------------------------------------------------------------------------
# 3VL 纯 Python 求值
# ---------------------------------------------------------------------------

def test_three_valued_logic_semantics():
    row = {"a": None, "b": None, "c": 7}
    assert eval_row({"gt": ["a", 1]}, row) is None          # NULL 比较 -> UNKNOWN
    assert eval_row({"is_null": "a"}, row) is True
    assert eval_row({"not": {"gt": ["a", 1]}}, row) is None  # NOT UNKNOWN -> UNKNOWN
    assert eval_row({"and": [{"is_null": "a"}, {"gt": ["a", 1]}]}, row) is None
    assert eval_row({"or": [{"gt": ["a", 1]}, {"is_null": "b"}]}, row) is True
    # FALSE AND UNKNOWN -> FALSE（FALSE 取自行 c 的确定性比较）；TRUE OR UNKNOWN -> TRUE
    assert eval_row({"and": [{"eq": ["c", 1]}, {"is_null": "b"}]}, row) is False
    assert eval_row({"or": [{"eq": ["c", 7]}, {"gt": ["a", 1]}]}, row) is True
    # 否定条件不会把 UNKNOWN 行当命中
    assert eval_row({"not": {"eq": ["a", 1]}}, row) is None


# ---------------------------------------------------------------------------
# 执行阶段：列读取请求、复用、无命中不读 payload
# ---------------------------------------------------------------------------

def _run_one(path, query):
    schema = pq.ParquetFile(path).schema_arrow
    validate(query.where, schema)
    groups = read_footer_stats(path, schema)
    decisions = {path: plan_file(path, query.where, groups)}
    out_path = os.path.join(os.path.dirname(path), "out.parquet")
    return scan_query(query, decisions, InstrumentedParquetFile, out_path)


def test_no_hit_group_never_reads_projection_columns(parquet_pair):
    stats_path, _ = parquet_pair
    q = parse_queries([{"id": "q", "where": {"gt": ["a", 10_000]}, "columns": ["id", "payload"]}])[0]
    run_obj = _run_one(stats_path, q)
    assert run_obj.result_rows == 0
    for g in run_obj.group_records:
        if g.status == "no_hits_after_filter":
            assert all(req.phase == "filter" for req in g.column_reads)
            assert all("payload" not in req.columns and "id" not in req.columns
                       for req in g.column_reads)
        else:
            assert g.status == "planned_skip" and g.column_reads == []


def test_filter_column_reused_when_projected(parquet_pair):
    stats_path, _ = parquet_pair
    # a 同时是过滤列和投影列：只允许在 filter 阶段读一次
    q = parse_queries([{"id": "q", "where": {"gt": ["a", 0]}, "columns": ["a", "payload"]}])[0]
    run_obj = _run_one(stats_path, q)
    for g in run_obj.group_records:
        if g.status != "matched":
            continue
        phases = [(req.phase, tuple(req.columns)) for req in g.column_reads]
        assert ("filter", ("a",)) in phases
        assert all("a" not in cols for phase, cols in phases if phase == "projection")
    assert run_obj.result_rows == 3  # 5, 100, 200（NULL 行不算）


def test_full_null_group_filtered_not_crashed(parquet_pair):
    stats_path, _ = parquet_pair
    q = parse_queries([{"id": "q", "where": {"lt": ["a", 0]}, "columns": ["id"]}])[0]
    run_obj = _run_one(stats_path, q)
    # 仅 RG0 的 -5 命中；全 NULL 的 RG1 扫描后无命中
    statuses = {g.row_group: g.status for g in run_obj.group_records}
    assert statuses[1] == "no_hits_after_filter"
    assert run_obj.result_rows == 1


# ---------------------------------------------------------------------------
# 端到端：结果与无剪枝全扫描一致，且有/无统计文件结果相同、计划保守度不同
# ---------------------------------------------------------------------------

def test_end_to_end_matches_reference_and_conservative_plan(parquet_pair):
    stats_path, nostats_path = parquet_pair
    base = os.path.dirname(stats_path)
    files_json = os.path.join(base, "files.json")
    with open(files_json, "w", encoding="utf-8") as fh:
        json.dump([os.path.basename(stats_path), os.path.basename(nostats_path)], fh)
    queries = [
        {"id": "rng", "where": {"and": [{"ge": ["a", 0]}, {"lt": ["a", 50]}]}, "columns": ["id", "payload"]},
        {"id": "nul", "where": {"is_null": "a"}, "columns": ["id", "b"]},
        {"id": "neg", "where": {"not": {"eq": ["b", "X"]}}, "columns": ["id", "a"]},
        {"id": "orx", "where": {"or": [{"eq": ["b", "X"]}, {"gt": ["a", 150]}]}, "columns": ["id"]},
        {"id": "emp", "where": {"gt": ["a", 9999]}, "columns": ["id", "payload"]},
    ]
    queries_json = os.path.join(base, "queries.json")
    with open(queries_json, "w", encoding="utf-8") as fh:
        json.dump(queries, fh)
    out = os.path.join(base, "out")

    doc = run(str(files_json), str(queries_json), str(out))
    assert all(q["passed"] for q in doc["verification"]["queries"])

    # 空结果也必须产出 schema 完整的 Parquet
    empty = pq.read_table(os.path.join(out, "results", "emp.parquet"))
    assert empty.num_rows == 0
    assert empty.schema.names == ["id", "payload", "_source_file", "_source_row_group", "_source_row_in_group"]

    # 有统计文件存在跳过；无统计文件全部保留
    with open(os.path.join(out, "plans", "rng.json"), encoding="utf-8") as fh:
        plan = json.load(fh)
    per_file = {f["file"].split("/")[-1]: [g["action"] for g in f["row_groups"]] for f in plan["files"]}
    assert "skip" in per_file["with_stats.parquet"]
    assert per_file["no_stats.parquet"] == ["keep"] * 3

    # 两份文件上的结果行（去掉文件溯源）必须一致
    result = pq.read_table(os.path.join(out, "results", "rng.parquet")).to_pylist()
    per_source = {}
    for row in result:
        per_source.setdefault(row["_source_file"].split("/")[-1], []).append(
            (row["id"], row["_source_row_group"], row["_source_row_in_group"])
        )
    assert per_source["with_stats.parquet"] == per_source["no_stats.parquet"]

    # 顺序：按 files.json 文件顺序、行组顺序、组内物理行号排列（非字典序）
    order = {stats_path: 0, nostats_path: 1}
    keys = [(order[r["_source_file"]], r["_source_row_group"], r["_source_row_in_group"])
            for r in result]
    assert keys == sorted(keys)


def test_predicate_columns_collection():
    pred = {"and": [{"or": [{"eq": ["a", 1]}, {"is_null": "b"}]}, {"not": {"lt": ["c", 2]}}]}
    assert predicate_columns(pred) == {"a", "b", "c"}
