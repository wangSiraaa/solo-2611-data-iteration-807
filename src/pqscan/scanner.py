"""执行阶段：两阶段列读取 + 三值逻辑精确行选择 + 读取请求记录。

流程（每个被计划保留的行组）：
1. 只读过滤列，用 PyArrow 的 Kleene 三值逻辑表达式算出布尔掩码（NULL=UNKNOWN）；
2. 掩码为 True 的行才是命中行；没有命中行的行组到此为止，绝不读取投影专用列；
3. 有命中行时再读“剩余投影列”；过滤列若同时在投影中，直接复用第 1 步的数组，
   不会对同一列发起第二次读取请求。

读取计数由本模块的执行入口（InstrumentedParquetFile.read_row_group）在真实调用
发生时记录，计划器不参与填写；记录的是“工具发起的列读取请求”，不是操作系统
实际磁盘字节，也不使用 footer 里的压缩大小冒充字节数。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from .expr import Query, predicate_columns, to_arrow_expression
from .planner import GroupDecision

PROVENANCE_FILE = "_source_file"
PROVENANCE_RG = "_source_row_group"
PROVENANCE_ROW = "_source_row_in_group"
PROVENANCE_COLUMNS = (PROVENANCE_FILE, PROVENANCE_RG, PROVENANCE_ROW)


@dataclass
class ReadRequest:
    """一次实际发起的 read_row_group 列读取请求。"""

    file: str
    row_group: int
    phase: str                 # "filter" | "projection"
    columns: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "file": self.file,
            "row_group": self.row_group,
            "phase": self.phase,
            "columns": list(self.columns),
        }


class InstrumentedParquetFile:
    """ParquetFile 的薄封装，唯一目的是在真实读取入口登记列请求。

    use_threads=False 让每次 read_row_group 与登记的请求一一对应，结果确定。
    """

    def __init__(self, path: str):
        self.path = path
        self._pf = pq.ParquetFile(path)
        self.requests: list[ReadRequest] = []

    @property
    def schema_arrow(self) -> pa.Schema:
        return self._pf.schema_arrow

    def read_row_group(self, row_group: int, columns: list[str], phase: str) -> pa.Table | None:
        if not columns:
            # 没有列需要读取时不发起 Parquet 读取（例如投影完全由过滤列覆盖）。
            return None
        self.requests.append(
            ReadRequest(file=self.path, row_group=row_group, phase=phase, columns=tuple(columns))
        )
        return self._pf.read_row_group(row_group, columns=columns, use_threads=False)


@dataclass
class GroupScanRecord:
    file: str
    row_group: int
    num_rows: int
    status: str                 # "planned_skip" | "no_hits_after_filter" | "matched"
    matched_rows: int
    filter_columns: tuple[str, ...]
    projection_only_columns: tuple[str, ...]
    column_reads: list[ReadRequest] = field(default_factory=list)
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "file": self.file,
            "row_group": self.row_group,
            "num_rows": self.num_rows,
            "status": self.status,
            "matched_rows": self.matched_rows,
            "filter_columns": list(self.filter_columns),
            "projection_only_columns": list(self.projection_only_columns),
            "column_reads": [r.to_dict() for r in self.column_reads],
            "note": self.note,
        }


@dataclass
class QueryRun:
    query_id: str
    output_path: str
    result_rows: int
    group_records: list[GroupScanRecord]
    all_read_requests: list[ReadRequest]

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "output_path": self.output_path,
            "result_rows": self.result_rows,
            "groups": [g.to_dict() for g in self.group_records],
            "read_requests": [r.to_dict() for r in self.all_read_requests],
        }


def _empty_result(schema: pa.Schema, query: Query, output_path: str) -> pa.Table:
    """构造带溯源列的空表，schema 与有命中时完全一致。"""
    fields = [schema.field(c) for c in query.columns]
    fields += [
        pa.field(PROVENANCE_FILE, pa.string()),
        pa.field(PROVENANCE_RG, pa.int32()),
        pa.field(PROVENANCE_ROW, pa.int64()),
    ]
    return pa.table({f.name: pa.array([], type=f.type) for f in fields}, schema=pa.schema(fields))


def scan_query(
    query: Query,
    decisions_by_file: dict[str, list[GroupDecision]],
    reader_factory: Callable[[str], "InstrumentedParquetFile"],
    output_path: str,
) -> QueryRun:
    """按 files.json 的文件顺序、行组顺序、物理行序执行一条查询并写结果 Parquet。

    reader_factory(path) 每次返回一个全新的 InstrumentedParquetFile，
    使本查询的列读取请求与其他查询完全隔离、不跨查询累积。
    """
    filter_cols = tuple(sorted(predicate_columns(query.where)))
    projection_only = tuple(c for c in query.columns if c not in filter_cols)
    expr = to_arrow_expression(query.where)

    # 每条查询独立的读取器集合：请求日志只包含本查询真实发起的读取。
    readers = {path: reader_factory(path) for path in decisions_by_file}
    group_records: list[GroupScanRecord] = []
    batches: list[pa.Table] = []
    total_rows = 0

    for file, decisions in decisions_by_file.items():
        reader = readers[file]
        schema = reader.schema_arrow
        for decision in decisions:
            if decision.action == "skip":
                group_records.append(
                    GroupScanRecord(
                        file=file, row_group=decision.row_group, num_rows=decision.num_rows,
                        status="planned_skip", matched_rows=0,
                        filter_columns=filter_cols, projection_only_columns=projection_only,
                        note="计划阶段已证明无 TRUE 行，执行阶段不发起任何列读取",
                    )
                )
                continue

            rgi = decision.row_group
            # 阶段 1：只读过滤列（读取请求在入口处登记）。
            before = len(reader.requests)
            filter_tbl = reader.read_row_group(rgi, list(filter_cols), phase="filter")
            filter_reads = list(reader.requests[before:])

            # Kleene 三值逻辑：比较/IS NULL 对 NULL 输入产生 UNKNOWN 掩码，
            # Table.filter 默认丢弃 UNKNOWN 与 FALSE，只保留 TRUE 行；
            # 追加临时行号列后过滤，即可取回命中行在原行组内的物理行号。
            marked = filter_tbl.append_column("_idx", pa.array(range(filter_tbl.num_rows)))
            hit_indices = marked.filter(expr).column("_idx").to_pylist()

            if not hit_indices:
                group_records.append(
                    GroupScanRecord(
                        file=file, row_group=rgi, num_rows=decision.num_rows,
                        status="no_hits_after_filter", matched_rows=0,
                        filter_columns=filter_cols, projection_only_columns=projection_only,
                        column_reads=filter_reads,
                        note="过滤后无命中行，不读取任何投影专用列",
                    )
                )
                continue

            # 阶段 2：只为本组命中行读取剩余投影列；过滤列复用上一步的数据。
            before = len(reader.requests)
            payload_tbl = reader.read_row_group(rgi, list(projection_only), phase="projection")
            payload_reads = list(reader.requests[before:])

            arrays: dict[str, pa.Array] = {}
            for col in query.columns:
                if col in filter_cols:
                    arrays[col] = filter_tbl.column(col).take(pa.array(hit_indices))
                else:
                    arrays[col] = payload_tbl.column(col).take(pa.array(hit_indices))

            n = len(hit_indices)
            arrays[PROVENANCE_FILE] = pa.array([file] * n, type=pa.string())
            arrays[PROVENANCE_RG] = pa.array([rgi] * n, type=pa.int32())
            arrays[PROVENANCE_ROW] = pa.array(hit_indices, type=pa.int64())

            ordered_fields = [schema.field(c) for c in query.columns] + [
                pa.field(PROVENANCE_FILE, pa.string()),
                pa.field(PROVENANCE_RG, pa.int32()),
                pa.field(PROVENANCE_ROW, pa.int64()),
            ]
            out_table = pa.table(arrays, schema=pa.schema(ordered_fields))
            batches.append(out_table)
            total_rows += n

            group_records.append(
                GroupScanRecord(
                    file=file, row_group=rgi, num_rows=decision.num_rows,
                    status="matched", matched_rows=n,
                    filter_columns=filter_cols, projection_only_columns=projection_only,
                    column_reads=filter_reads + payload_reads,
                    note="过滤列在投影中时直接复用，未重复读取",
                )
            )

    if batches:
        result = pa.concat_tables(batches, promote_options="none")
    else:
        first_reader = next(iter(readers.values()))
        result = _empty_result(first_reader.schema_arrow, query, output_path)

    pq.write_table(result, output_path, write_statistics=True)

    all_requests = [req for reader in readers.values() for req in reader.requests]
    return QueryRun(
        query_id=query.id, output_path=output_path, result_rows=total_rows,
        group_records=group_records, all_read_requests=all_requests,
    )
