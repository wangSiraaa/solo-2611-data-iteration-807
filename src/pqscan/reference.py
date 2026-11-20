"""独立核对：无剪枝完整扫描。

与 planner/scanner 完全不同的代码路径：
- 不读取也不相信任何 footer 统计，每个行组全量读取全部列；
- 用 expr.eval_row 的纯 Python 三值逻辑逐行求值，只有 TRUE 命中，UNKNOWN 排除；
- 同样按 files.json 顺序、行组顺序、物理行序产出，便于逐行比对
  结果内容、顺序与溯源信息。
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from .expr import Query, eval_row
from .scanner import PROVENANCE_FILE, PROVENANCE_RG, PROVENANCE_ROW


def reference_scan(query: Query, files: list[str]) -> pa.Table:
    """对文件清单做无剪枝全扫描，返回与 scanner 同 schema 的结果表。"""
    batches: list[pa.Table] = []

    for path in files:
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow
        for rgi in range(pf.num_row_groups):
            tbl = pf.read_row_group(rgi)  # 全列、全量、无统计推理
            rows = tbl.to_pylist()
            hit_indices = [i for i, row in enumerate(rows) if eval_row(query.where, row) is True]
            if not hit_indices:
                continue
            arrays = {
                col: tbl.column(col).take(pa.array(hit_indices)) for col in query.columns
            }
            n = len(hit_indices)
            arrays[PROVENANCE_FILE] = pa.array([path] * n, type=pa.string())
            arrays[PROVENANCE_RG] = pa.array([rgi] * n, type=pa.int32())
            arrays[PROVENANCE_ROW] = pa.array(hit_indices, type=pa.int64())
            fields = [schema.field(c) for c in query.columns] + [
                pa.field(PROVENANCE_FILE, pa.string()),
                pa.field(PROVENANCE_RG, pa.int32()),
                pa.field(PROVENANCE_ROW, pa.int64()),
            ]
            batches.append(pa.table(arrays, schema=pa.schema(fields)))

    if not batches:
        any_schema = pq.ParquetFile(files[0]).schema_arrow
        fields = [any_schema.field(c) for c in query.columns] + [
            pa.field(PROVENANCE_FILE, pa.string()),
            pa.field(PROVENANCE_RG, pa.int32()),
            pa.field(PROVENANCE_ROW, pa.int64()),
        ]
        return pa.table({f.name: pa.array([], type=f.type) for f in fields}, schema=pa.schema(fields))

    return pa.concat_tables(batches, promote_options="none")
