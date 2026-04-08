"""Enhanced dataset profiling for intelligent routing decisions."""

from __future__ import annotations

from data_quality.tools.profiler import profile_dataset


def create_dataset_profile(csv_path: str) -> dict:
    """Create a comprehensive dataset profile used by the top-level router.

    The return payload preserves the existing profile fields while adding
    routing-oriented aggregates.
    """
    base = profile_dataset(csv_path)
    columns = base.get("columns", {})

    numeric_columns = [c for c, meta in columns.items() if meta.get("semantic_type") == "numeric"]
    categorical_columns = [
        c
        for c, meta in columns.items()
        if meta.get("semantic_type") in ("categorical", "text", "identifier", "boolean")
    ]
    date_columns = [c for c, meta in columns.items() if meta.get("semantic_type") == "date"]

    if columns:
        overall_completeness = (
            sum(100 - float(meta.get("null_pct", 0.0)) for meta in columns.values())
            / len(columns)
        )
    else:
        overall_completeness = 100.0

    estimated_issues = {
        "high_missing_columns": [c for c, meta in columns.items() if float(meta.get("null_pct", 0.0)) > 20.0],
        "ultra_sparse_columns": [c for c, meta in columns.items() if float(meta.get("null_pct", 0.0)) > 95.0],
        "possible_identifier_columns": [
            c for c, meta in columns.items() if float(meta.get("cardinality_ratio", 0.0)) > 0.95
        ],
    }

    return {
        **base,
        "columns_list": list(columns.keys()),
        "total_columns": base.get("column_count", 0),
        "total_rows": base.get("row_count", 0),
        "overall_completeness": round(overall_completeness / 100.0, 4),
        "overall_completeness_pct": round(overall_completeness, 2),
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "date_columns": date_columns,
        "estimated_issues": estimated_issues,
    }
