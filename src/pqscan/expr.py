"""查询表达式：解析 queries.json 的谓词树、schema 校验、PyArrow 表达式构造，
以及供独立核对使用的纯 Python 三值逻辑（3VL）逐行求值。

谓词树格式（JSON 友好的前缀式）：
    {"eq":  ["col", value]}   # 另支持 ne / lt / le / gt / ge
    {"is_null": "col"}
    {"not": <predicate>}
    {"and": [<predicate>, ...]}
    {"or":  [<predicate>, ...]}

只支持平面 schema、整数（int32/int64）与字符串（string/large_string）列。
比较字面量类型必须与列类型同族，否则在计划期报错。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from .errors import PlanError

COMPARE_OPS = ("eq", "ne", "lt", "le", "gt", "ge")
LOGICAL_OPS = ("and", "or")

# 比较运算符 -> PyArrow compute 函数名（这些函数对 NULL 输入返回 NULL，即 3VL）。
_PC_COMPARE = {
    "eq": "equal",
    "ne": "not_equal",
    "lt": "less",
    "le": "less_equal",
    "gt": "greater",
    "ge": "greater_equal",
}


@dataclass(frozen=True)
class Query:
    """一条查询：id、谓词树（原始 dict）、投影列（不含溯源列）。"""

    id: str
    where: dict[str, Any]
    columns: tuple[str, ...]


def parse_queries(raw: list[dict[str, Any]]) -> list[Query]:
    queries: list[Query] = []
    for item in raw:
        if not isinstance(item, dict) or "id" not in item or "where" not in item:
            raise PlanError(f"查询必须是包含 id/where 的对象: {item!r}")
        cols = item.get("columns", [])
        if not isinstance(cols, list) or not all(isinstance(c, str) for c in cols):
            raise PlanError(f"查询 {item['id']} 的 columns 必须是字符串列表")
        queries.append(Query(id=str(item["id"]), where=item["where"], columns=tuple(cols)))
    return queries


def validate(predicate: Any, schema: pa.Schema) -> None:
    """对谓词树做结构性与 schema 校验，不合法直接抛 PlanError。"""
    if not isinstance(predicate, dict) or len(predicate) != 1:
        raise PlanError(f"谓词节点必须是单键对象: {predicate!r}")
    (op, body), = predicate.items()

    if op in COMPARE_OPS:
        if not isinstance(body, list) or len(body) != 2 or not isinstance(body[0], str):
            raise PlanError(f"{op} 的形式必须是 [列名, 字面量]: {body!r}")
        col, literal = body
        if col not in schema.names:
            raise PlanError(f"谓词引用了不存在的列: {col}")
        _check_literal_type(op, col, literal, schema.field(col).type)
        return

    if op == "is_null":
        if not isinstance(body, str) or body not in schema.names:
            raise PlanError(f"is_null 的形式必须是列名且列存在: {body!r}")
        return

    if op == "not":
        validate(body, schema)
        return

    if op in LOGICAL_OPS:
        if not isinstance(body, list) or not body:
            raise PlanError(f"{op} 必须是非空谓词列表: {body!r}")
        for child in body:
            validate(child, schema)
        return

    raise PlanError(f"不支持的谓词运算符: {op}")


def _check_literal_type(op: str, col: str, literal: Any, col_type: pa.DataType) -> None:
    if pa.types.is_integer(col_type):
        if isinstance(literal, bool) or not isinstance(literal, int):
            raise PlanError(f"{op}: 整数列 {col} 不能与字面量 {literal!r} 比较")
    elif pa.types.is_string(col_type) or pa.types.is_large_string(col_type):
        if not isinstance(literal, str):
            raise PlanError(f"{op}: 字符串列 {col} 不能与字面量 {literal!r} 比较")
    else:
        raise PlanError(f"列 {col} 的类型 {col_type} 不支持比较（仅整数/字符串）")


def validate_projection(columns: tuple[str, ...], schema: pa.Schema) -> None:
    for col in columns:
        if col not in schema.names:
            raise PlanError(f"投影列不存在: {col}")


def predicate_columns(predicate: Any) -> set[str]:
    """收集谓词引用到的全部列名。"""
    (op, body), = predicate.items()
    if op in COMPARE_OPS:
        return {body[0]}
    if op == "is_null":
        return {body}
    if op == "not":
        return predicate_columns(body)
    cols: set[str] = set()
    for child in body:
        cols |= predicate_columns(child)
    return cols


def to_arrow_expression(predicate: dict[str, Any]) -> pc.Expression:
    """把谓词树转成 PyArrow 表达式。

    PyArrow 的 and_kleene/or_kleene/not_ 与比较函数都遵守 SQL 三值逻辑，
    因此该表达式直接用于行级精确过滤；这里只做机械翻译，不做任何下推/剪枝。
    """
    (op, body), = predicate.items()
    if op in COMPARE_OPS:
        col, literal = body
        return getattr(pc, _PC_COMPARE[op])(pc.field(col), pc.scalar(literal))
    if op == "is_null":
        return pc.field(body).is_null()
    if op == "not":
        # Expression.__invert__ 编译为 invert（NOT），NULL 输入仍得到 NULL。
        return ~to_arrow_expression(body)
    if op == "and":
        expr = to_arrow_expression(body[0])
        for child in body[1:]:
            expr = pc.and_kleene(expr, to_arrow_expression(child))
        return expr
    if op == "or":
        expr = to_arrow_expression(body[0])
        for child in body[1:]:
            expr = pc.or_kleene(expr, to_arrow_expression(child))
        return expr
    raise PlanError(f"不支持的谓词运算符: {op}")


# ---------------------------------------------------------------------------
# 纯 Python 3VL 逐行求值，供 reference 全扫描独立核对，不依赖 PyArrow 表达式。
# True/False/None 分别对应 TRUE/FALSE/UNKNOWN。
# ---------------------------------------------------------------------------

def eval_row(predicate: dict[str, Any], row: dict[str, Any]) -> bool | None:
    (op, body), = predicate.items()

    if op in COMPARE_OPS:
        col, literal = body
        value = row[col]
        if value is None:
            return None
        if op == "eq":
            result = value == literal
        elif op == "ne":
            result = value != literal
        elif op == "lt":
            result = value < literal
        elif op == "le":
            result = value <= literal
        elif op == "gt":
            result = value > literal
        else:  # ge
            result = value >= literal
        return bool(result)

    if op == "is_null":
        return row[body] is None

    if op == "not":
        inner = eval_row(body, row)
        return None if inner is None else not inner

    if op == "and":
        result: bool | None = True
        saw_unknown = False
        for child in body:
            value = eval_row(child, row)
            if value is False:
                return False
            if value is None:
                saw_unknown = True
            elif value is True:
                continue
        return None if saw_unknown else True

    # or
    saw_unknown = False
    for child in body:
        value = eval_row(child, row)
        if value is True:
            return True
        if value is None:
            saw_unknown = True
    return None if saw_unknown else False
