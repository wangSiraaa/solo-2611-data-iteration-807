"""从同一份内存数据生成 stats.parquet 与 no-stats.parquet。

两个文件的行组划分、行顺序、数据内容完全一致；区别仅在于写入时是否生成
footer 统计（ParquetWriter.write_statistics）。这样可以验证：
- 无统计文件的查询结果必须与有统计文件一致；
- 无统计文件上的计划必须更保守（不跳过任何行组）。

直接运行：python tools/make_fixtures.py
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        pa.field("row_id", pa.int64()),
        pa.field("k", pa.int64()),
        pa.field("kind", pa.string()),
        pa.field("amount", pa.int64()),
        pa.field("payload", pa.string()),
    ]
)

# 5 个行组，每组 4 行，共 20 行。
# 第 3 组（索引 2）的 k 全为 NULL，是“全 NULL 组”。
ROWS = [
    # row_id, k, kind, amount
    (0, None, None, None),
    (1, -7, "B", 11),
    (2, -4, "C", 22),
    (3, None, "A", 33),
    (4, 2, "B", None),
    (5, 5, None, 55),
    (6, 8, "A", 66),
    (7, 11, "B", 77),
    (8, None, "C", None),
    (9, None, "A", 99),
    (10, None, None, 110),
    (11, None, "C", 121),
    (12, 26, "A", None),
    (13, 29, "B", 143),
    (14, 32, "C", 154),
    (15, 35, None, 165),
    (16, 38, "B", None),
    (17, 41, "C", 187),
    (18, 44, "A", 198),
    (19, 47, "B", 209),
]
ROWS_PER_GROUP = 4
PAYLOAD_PAD = "x" * 72


def _table_for_group(start: int) -> pa.Table:
    chunk = ROWS[start : start + ROWS_PER_GROUP]
    return pa.table(
        {
            "row_id": [r[0] for r in chunk],
            "k": [r[1] for r in chunk],
            "kind": [r[2] for r in chunk],
            "amount": [r[3] for r in chunk],
            "payload": [f"行{r[0]}/{PAYLOAD_PAD}" for r in chunk],
        },
        schema=SCHEMA,
    )


def write(path: str, *, write_statistics: bool) -> None:
    writer = pq.ParquetWriter(path, SCHEMA, write_statistics=write_statistics)
    try:
        for start in range(0, len(ROWS), ROWS_PER_GROUP):
            writer.write_table(_table_for_group(start))
    finally:
        writer.close()


def main() -> None:
    write("fixtures/stats.parquet", write_statistics=True)
    write("fixtures/no-stats.parquet", write_statistics=False)
    print("wrote fixtures/stats.parquet and fixtures/no-stats.parquet")


if __name__ == "__main__":
    main()
