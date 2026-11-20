"""从 Parquet footer 提取行组级统计。

本模块只访问 FileMetaData / RowGroupMetaData / Statistics，即 footer 内容，
不触发任何列数据读取。统计的“可靠性”按以下规则保守判定：

- 没有 Statistics 对象、has_min_max=False、has_null_count=False：对应部分缺失；
- 列物理类型不在整数/字符串范围内：min/max 不参与比较推理（本工具只支持这两类）；
- PyArrow 19 的 Statistics 未暴露截断标记（无 is_min_max_exact 属性），
  因此对存在且类型受支持的 min/max 视为精确；一旦环境提供了截断信息，
  应在此处把截断统计降级为不可靠（ColumnStats.min_max_reliable=False）。
"""
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import PlanError

_SUPPORTED_PHYSICAL = {"INT32", "INT64", "BYTE_ARRAY"}


@dataclass(frozen=True)
class ColumnStats:
    """单个行组内单列的 footer 统计视图。"""

    column: str
    num_rows: int
    null_count: int | None            # None = footer 未提供
    min: object | None                # 逻辑类型 min（可能为 None 表示全 NULL）
    max: object | None
    min_max_reliable: bool            # False = 缺失/不可靠，比较推理禁止使用
    null_count_reliable: bool
    unknown_reasons: tuple[str, ...]  # 人读的未知项说明


@dataclass(frozen=True)
class RowGroupStats:
    row_group_index: int
    num_rows: int
    columns: dict[str, ColumnStats]


def _column_stats(col_meta: pq.ColumnChunkMetaData, num_rows: int) -> ColumnStats:
    name = col_meta.path_in_schema
    st = col_meta.statistics
    reasons: list[str] = []

    null_count: int | None = None
    null_reliable = False
    min_max_reliable = False
    min_v = max_v = None

    if st is None:
        reasons.append("footer 中没有该列的 Statistics 块")
    else:
        if st.has_null_count:
            null_count = st.null_count
            null_reliable = True
        else:
            reasons.append("null_count 缺失")

        physical = col_meta.physical_type
        if physical not in _SUPPORTED_PHYSICAL:
            reasons.append(f"物理类型 {physical} 不受支持，min/max 不参与推理")
        elif not st.has_min_max:
            reasons.append("min/max 缺失（该列在本行组可能全为 NULL）")
        else:
            min_v, max_v = st.min, st.max
            min_max_reliable = True

    return ColumnStats(
        column=name,
        num_rows=num_rows,
        null_count=null_count,
        min=min_v,
        max=max_v,
        min_max_reliable=min_max_reliable,
        null_count_reliable=null_reliable,
        unknown_reasons=tuple(reasons),
    )


def read_footer_stats(path: str, schema: pa.Schema) -> list[RowGroupStats]:
    """打开文件只读 footer，返回每个行组的统计视图。

    schema 由调用方从同一 footer 取得后传入，用于校验平面结构。
    """
    pf = pq.ParquetFile(path)
    if any(pa.types.is_nested(t) for t in pf.schema_arrow.types):
        raise PlanError(f"{path}: 只支持平面 schema，发现嵌套列")

    groups: list[RowGroupStats] = []
    for rgi in range(pf.num_row_groups):
        rg_meta = pf.metadata.row_group(rgi)
        cols = {
            rg_meta.column(ci).path_in_schema: _column_stats(rg_meta.column(ci), rg_meta.num_rows)
            for ci in range(rg_meta.num_columns)
        }
        groups.append(RowGroupStats(rgi, rg_meta.num_rows, cols))
    return groups
