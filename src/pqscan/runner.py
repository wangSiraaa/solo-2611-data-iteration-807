"""执行入口：加载输入 -> footer 计划 -> 两阶段扫描 -> 独立全扫描核对 -> 落盘。

输出物（均在 output/ 下）：
- plans/<query_id>.json   ：行组选择说明（采用的统计、推理路径、未知项、决策）；
- results/<query_id>.parquet：查询结果（含溯源列）；
- run_record.json         ：实际发起的行组/列读取请求与三类行组状态；
- verification.json       ：无剪枝全扫描的逐行核对结论。
"""
from __future__ import annotations

import json
import os
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import PlanError
from .expr import parse_queries, predicate_columns, validate, validate_projection
from .planner import GroupDecision, plan_file
from .reference import reference_scan
from .scanner import InstrumentedParquetFile, PROVENANCE_COLUMNS, scan_query
from .stats import read_footer_stats


def _load_json(path: str) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _plan_to_dict(query_id: str, predicate: dict[str, Any],
                  decisions_by_file: dict[str, list[GroupDecision]]) -> dict[str, Any]:
    return {
        "query_id": query_id,
        "predicate": predicate,
        "rule": "仅当能证明 can_be_true=False 时跳过；统计缺失/不可靠则保守保留；"
                "NOT 同时翻转 can_be_true/can_be_false，UNKNOWN 不当 FALSE",
        "files": [
            {
                "file": path,
                "row_groups": [
                    {
                        "row_group": d.row_group,
                        "num_rows": d.num_rows,
                        "action": d.action,
                        "reason": d.reason,
                        "can_be_true": d.can_be_true,
                        "can_be_false": d.can_be_false,
                        "missing_or_unreliable_stats": list(d.missing_stats),
                        "trace": d.trace.to_dict(),
                    }
                    for d in decisions
                ],
            }
            for path, decisions in decisions_by_file.items()
        ],
    }


def run(files_json: str, queries_json: str, out_dir: str) -> dict[str, Any]:
    fixture_dir = os.path.dirname(os.path.abspath(files_json))
    file_names = _load_json(files_json)
    files = [os.path.join(fixture_dir, name) for name in file_names]
    queries = parse_queries(_load_json(queries_json))

    plans_dir = os.path.join(out_dir, "plans")
    results_dir = os.path.join(out_dir, "results")
    os.makedirs(plans_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)

    # footer 只打开一次：统计提取与 schema 校验共用同一批 footer 视图；
    # 执行阶段每条查询通过工厂拿到全新的读取器，请求按查询隔离、不跨查询累积。
    footer_pf = {path: pq.ParquetFile(path) for path in files}
    schemas = {path: pf.schema_arrow for path, pf in footer_pf.items()}
    footer_stats = {path: read_footer_stats(path, schemas[path]) for path in files}

    run_record: dict[str, Any] = {
        "note": "记录工具实际发起的 read_row_group 列读取请求；非操作系统磁盘字节，"
                "也不使用 footer 压缩大小。计数由扫描执行入口在调用时即时登记。",
        "files": file_names,
        "queries": [],
    }
    verification: dict[str, Any] = {"queries": []}

    for query in queries:
        for path in files:
            validate(query.where, schemas[path])
            validate_projection(query.columns, schemas[path])
        filter_cols = sorted(predicate_columns(query.where))
        if set(filter_cols) & set(PROVENANCE_COLUMNS):
            raise PlanError(f"查询 {query.id} 引用了保留的溯源列名 {PROVENANCE_COLUMNS}")

        # ---- 计划阶段：只用 footer 统计 ------------------------------------
        decisions_by_file = {
            path: plan_file(path, query.where, footer_stats[path]) for path in files
        }
        plan_doc = _plan_to_dict(query.id, query.where, decisions_by_file)
        with open(os.path.join(plans_dir, f"{query.id}.json"), "w", encoding="utf-8") as fh:
            json.dump(plan_doc, fh, ensure_ascii=False, indent=2)

        # ---- 执行阶段：真实列读取在 scan_query 内发生并登记 -----------------
        output_path = os.path.join(results_dir, f"{query.id}.parquet")
        query_run = scan_query(query, decisions_by_file, InstrumentedParquetFile, output_path)
        run_record["queries"].append(query_run.to_dict())

        # ---- 独立核对：无剪枝全扫描（纯 Python 3VL）------------------------
        ref_table = reference_scan(query, files)
        got_table = pq.read_table(output_path)
        ok, detail = _compare(got_table, ref_table)
        verification["queries"].append(
            {
                "query_id": query.id,
                "passed": ok,
                "detail": detail,
                "engine_rows": got_table.num_rows,
                "reference_rows": ref_table.num_rows,
            }
        )

    with open(os.path.join(out_dir, "run_record.json"), "w", encoding="utf-8") as fh:
        json.dump(run_record, fh, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "verification.json"), "w", encoding="utf-8") as fh:
        json.dump(verification, fh, ensure_ascii=False, indent=2)

    return {"run_record": run_record, "verification": verification}


def _compare(got: pa.Table, expected: pa.Table) -> tuple[bool, str]:
    if got.schema != expected.schema:
        return False, f"schema 不一致: {got.schema} != {expected.schema}"
    if got.num_rows != expected.num_rows:
        return False, f"行数不一致: {got.num_rows} != {expected.num_rows}"
    if got.to_pylist() != expected.to_pylist():
        return False, "逐行内容/顺序/溯源不一致"
    return True, "结果内容、物理行序与溯源列与无剪枝全扫描完全一致"
