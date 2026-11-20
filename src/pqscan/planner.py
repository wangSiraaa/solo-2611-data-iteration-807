"""计划阶段：只读 footer 统计，保守判断每个行组“是否可能存在 TRUE 行”。

推理在 SQL 三值逻辑（TRUE / FALSE / UNKNOWN）上进行，对每个谓词子树求两个
结论：
- can_be_true : 行组内是否可能有某行让该子树为 TRUE
- can_be_false: 行组内是否可能有某行让该子树为 FALSE
UNKNOWN 行既不是 TRUE 也不是 FALSE。关键约束：
1. 只有能证明 can_be_true=False 时才能跳过行组；
2. NOT 必须同时翻转两个结论，绝不能把 UNKNOWN 当 FALSE 再取反；
3. AND/OR 只做独立子树结论的保守组合，不假设不同列的极值来自同一行；
4. 统计缺失/不可靠时，叶子结论回退为 (True, True)，行组保留。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .expr import COMPARE_OPS
from .stats import ColumnStats, RowGroupStats


@dataclass
class TraceNode:
    """谓词子树在单个行组上的推理轨迹（用于计划说明）。"""

    op: str
    detail: str
    can_be_true: bool
    can_be_false: bool
    stats_used: dict[str, dict[str, Any]] = field(default_factory=dict)
    unknown: list[str] = field(default_factory=list)
    children: list["TraceNode"] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "op": self.op,
            "detail": self.detail,
            "can_be_true": self.can_be_true,
            "can_be_false": self.can_be_false,
        }
        if self.stats_used:
            out["stats_used"] = self.stats_used
        if self.unknown:
            out["unknown"] = self.unknown
        if self.children:
            out["children"] = [c.to_dict() for c in self.children]
        return out


@dataclass(frozen=True)
class GroupDecision:
    file: str
    row_group: int
    num_rows: int
    action: str               # "keep" | "skip"
    reason: str
    can_be_true: bool
    can_be_false: bool
    trace: TraceNode
    missing_stats: tuple[str, ...]   # 谓词涉及但统计缺失/不可靠的列


def _stats_snapshot(cs: ColumnStats) -> dict[str, Any]:
    snap: dict[str, Any] = {
        "num_rows": cs.num_rows,
        "null_count": cs.null_count,
        "min": cs.min,
        "max": cs.max,
        "min_max_reliable": cs.min_max_reliable,
        "null_count_reliable": cs.null_count_reliable,
    }
    if cs.unknown_reasons:
        snap["unknown_reasons"] = list(cs.unknown_reasons)
    return snap


def _compare_outcomes(op: str, cs: ColumnStats, literal: Any) -> tuple[bool, bool]:
    """基于可靠 min/max 求比较谓词的 (can_be_true, can_be_false)。

    调用方保证 cs.min_max_reliable 为 True（has_min_max 意味着至少一个非 NULL）。
    NULL 行结果为 UNKNOWN，不计入 true/false；false 结论只由非 NULL 值域推出。
    """
    lo, hi = cs.min, cs.max

    if op == "eq":
        can_true = lo <= literal <= hi
        can_false = lo != literal or hi != literal
    elif op == "ne":
        can_true = lo != literal or hi != literal
        can_false = lo <= literal <= hi
    elif op == "lt":
        can_true = lo < literal
        can_false = hi >= literal
    elif op == "le":
        can_true = lo <= literal
        can_false = hi > literal
    elif op == "gt":
        can_true = hi > literal
        can_false = lo <= literal
    else:  # ge
        can_true = hi >= literal
        can_false = lo < literal
    return can_true, can_false


def _solve(predicate: dict[str, Any], rg: RowGroupStats) -> tuple[bool, bool, TraceNode]:
    (op, body), = predicate.items()

    # ---- 比较叶子 ----------------------------------------------------------
    if op in COMPARE_OPS:
        col, literal = body
        cs = rg.columns[col]
        used = {col: _stats_snapshot(cs)}
        detail = f"{col} {op} {literal!r}"

        if not cs.min_max_reliable:
            # 统计缺失/不可靠：无法排除任何取值，保守保留两种可能。
            node = TraceNode(
                op=op, detail=detail, can_be_true=True, can_be_false=True,
                stats_used=used, unknown=[f"列 {col} 的 min/max 不可靠，结论退化为 UNKNOWN 保守保留"],
            )
            return True, True, node

        can_true, can_false = _compare_outcomes(op, cs, literal)
        node = TraceNode(
            op=op, detail=detail, can_be_true=can_true, can_be_false=can_false,
            stats_used=used,
        )
        return can_true, can_false, node

    # ---- IS NULL / IS NOT NULL --------------------------------------------
    if op == "is_null":
        col = body
        cs = rg.columns[col]
        used = {col: _stats_snapshot(cs)}
        detail = f"{col} IS NULL"
        if not cs.null_count_reliable:
            node = TraceNode(
                op=op, detail=detail, can_be_true=True, can_be_false=True,
                stats_used=used, unknown=[f"列 {col} 的 null_count 缺失，结论退化为 UNKNOWN 保守保留"],
            )
            return True, True, node
        nc = cs.null_count or 0
        can_true = nc > 0
        can_false = nc < cs.num_rows
        node = TraceNode(op=op, detail=detail, can_be_true=can_true, can_be_false=can_false,
                         stats_used=used)
        return can_true, can_false, node

    # ---- NOT：两个结论一起翻转，UNKNOWN 不会被当成 FALSE --------------------
    if op == "not":
        inner_t, inner_f, child = _solve(body, rg)
        can_true, can_false = inner_f, inner_t
        node = TraceNode(
            op="not", detail=f"NOT ({child.detail})",
            can_be_true=can_true, can_be_false=can_false,
            unknown=list(child.unknown), children=[child],
        )
        return can_true, can_false, node

    # ---- AND / OR：独立子树的保守组合，不跨列关联极值 -----------------------
    children: list[TraceNode] = []
    parts: list[tuple[bool, bool]] = []
    for child_pred in body:
        t, f, node = _solve(child_pred, rg)
        parts.append((t, f))
        children.append(node)

    if op == "and":
        # AND 为 TRUE 需要所有子树同时 TRUE，无法证明联合可行 -> 只要任一子树
        # 不可能 TRUE，整体就不可能 TRUE。
        can_true = all(t for t, _ in parts)
        # 任一子树可能 FALSE，AND 就可能 FALSE（NULL 不构成 FALSE）。
        can_false = any(f for _, f in parts)
        detail = " AND ".join(c.detail for c in children)
    else:  # or
        # 任一子树可能 TRUE，OR 即可能 TRUE。
        can_true = any(t for t, _ in parts)
        # OR 为 FALSE 需要所有子树同时 FALSE。
        can_false = all(f for _, f in parts)
        detail = " OR ".join(c.detail for c in children)

    node = TraceNode(op=op, detail=detail, can_be_true=can_true, can_be_false=can_false,
                     children=children)
    return can_true, can_false, node


def _collect_missing(predicate: dict[str, Any], rg: RowGroupStats, seen: set[str]) -> set[str]:
    (op, body), = predicate.items()
    if op in COMPARE_OPS:
        col = body[0]
        if col not in seen:
            seen.add(col)
            if not rg.columns[col].min_max_reliable:
                return {col}
        return set()
    if op == "is_null":
        col = body
        if col not in seen:
            seen.add(col)
            if not rg.columns[col].null_count_reliable:
                return {col}
        return set()
    if op == "not":
        return _collect_missing(body, rg, seen)
    out: set[str] = set()
    for child in body:
        out |= _collect_missing(child, rg, seen)
    return out


def plan_file(path: str, predicate: dict[str, Any], groups: list[RowGroupStats]) -> list[GroupDecision]:
    decisions: list[GroupDecision] = []
    for rg in groups:
        can_true, can_false, trace = _solve(predicate, rg)
        missing = tuple(sorted(_collect_missing(predicate, rg, set())))
        if can_true:
            if missing:
                reason = (
                    f"保留：统计缺失/不可靠（{', '.join(missing)}），无法证明不存在 TRUE 行，保守保留"
                )
            elif can_false:
                reason = "保留：统计表明行组内可能存在 TRUE 行（同时也可能有 FALSE/NULL 行），需扫描精确定位"
            else:
                reason = "保留：统计表明组内所有非 NULL 行都可能满足谓词，仍需扫描精确定位"
            action = "keep"
        else:
            reason = "跳过：已证明行组内不存在任何 TRUE 行（NULL 对该谓词只产生 UNKNOWN）"
            action = "skip"
        decisions.append(
            GroupDecision(
                file=path, row_group=rg.row_group_index, num_rows=rg.num_rows,
                action=action, reason=reason, can_be_true=can_true, can_be_false=can_false,
                trace=trace, missing_stats=missing,
            )
        )
    return decisions
