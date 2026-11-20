"""异常类型。"""
from __future__ import annotations


class PqScanError(Exception):
    """所有工具层错误的基类（不用于损坏 Parquet 恢复，仅做输入校验）。"""


class PlanError(PqScanError):
    """查询表达式或 schema 不支持。"""
