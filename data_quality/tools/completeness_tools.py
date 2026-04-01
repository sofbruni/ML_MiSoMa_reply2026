"""
Completeness Analysis tools:
  - detect_missing_values       : counts nulls + placeholder strings per column/row
  - calculate_completeness_rate : % complete per column and overall
  - detect_sparse_columns       : finds columns >50% empty
  - apply_completeness_fixes    : (non-tool) fills missing values where safe
"""

import json
import pandas as pd
from langchain_core.tools import tool
from data_quality.config import PLACEHOLDER_VALUES


def _load_with_placeholders_as_nan(path: str) -> pd.DataFrame:
    """Load CSV and replace all known placeholder strings with NaN."""
    df = pd.read_csv(path, dtype=str)
    return df.replace(list(PLACEHOLDER_VALUES), pd.NA)


@tool
def detect_missing_values(dataset_path: str) -> str:
    """Detect null, empty, and placeholder values per column and report affected row counts.
    Returns a JSON findings report."""
    df = _load_with_placeholders_as_nan(dataset_path)

    missing_per_col: dict[str, dict] = {}
    for col in df.columns:
        null_count = int(df[col].isna().sum())
        if null_count > 0:
            pct = round(null_count / len(df) * 100, 2)
            missing_per_col[col] = {"null_count": null_count, "null_pct": pct}

    rows_with_any_null = int(df.isna().any(axis=1).sum())

    return json.dumps({
        "check": "null_missing_value_detection",
        "missing_per_column": missing_per_col,
        "rows_with_any_null": rows_with_any_null,
        "total_rows": len(df),
        "status": "issues_found" if missing_per_col else "ok",
    }, indent=2)


@tool
def calculate_completeness_rate(dataset_path: str) -> str:
    """Compute the completeness percentage per column and overall.
    Returns a JSON findings report."""
    df = _load_with_placeholders_as_nan(dataset_path)
    n = len(df)

    per_col = {
        col: round((1 - df[col].isna().sum() / n) * 100, 2)
        for col in df.columns
    }
    overall = round(sum(per_col.values()) / len(per_col), 2)

    return json.dumps({
        "check": "completeness_rate_calculation",
        "completeness_per_column_pct": per_col,
        "overall_completeness_pct": overall,
        "status": "ok",
    }, indent=2)


@tool
def detect_sparse_columns(dataset_path: str, threshold: float = 0.5) -> str:
    """Identify columns where more than `threshold` fraction of values are missing.
    Default threshold = 0.5 (50%). Returns a JSON findings report."""
    df = _load_with_placeholders_as_nan(dataset_path)
    n = len(df)

    sparse: dict[str, float] = {}
    for col in df.columns:
        missing_pct = df[col].isna().sum() / n
        if missing_pct > threshold:
            sparse[col] = round(missing_pct * 100, 2)

    return json.dumps({
        "check": "sparse_column_detection",
        "threshold_pct": threshold * 100,
        "sparse_columns": sparse,   # col → % missing
        "status": "issues_found" if sparse else "ok",
    }, indent=2)


# ---------------------------------------------------------------------------
# Fix function (called by the completeness team node, not by the LLM)
# ---------------------------------------------------------------------------

def apply_completeness_fixes(input_path: str, output_path: str) -> dict:
    """
    Applies completeness corrections:
      1. Replaces placeholder strings with NaN
      2. Drops columns that are >95% empty (note/fonte_dato)
      3. Fills numeric columns with their median
      4. Fills categorical columns with 'Unknown'
    Returns a summary dict.
    """
    df = pd.read_csv(input_path, dtype=str)
    changes = []

    # 1. Replace placeholders with NaN
    before_nulls = int(df.isna().sum().sum())
    df = df.replace(list(PLACEHOLDER_VALUES), pd.NA)
    after_nulls = int(df.isna().sum().sum())
    if after_nulls > before_nulls:
        changes.append(f"Converted {after_nulls - before_nulls} placeholder strings to NaN")

    # 2. Drop columns >95% empty
    n = len(df)
    ultra_sparse = [c for c in df.columns if df[c].isna().sum() / n > 0.95]
    if ultra_sparse:
        df = df.drop(columns=ultra_sparse)
        changes.append(f"Dropped ultra-sparse columns (>95% missing): {ultra_sparse}")

    # 3. Coerce numeric columns and fill with median
    numeric_cols = ["spesa", "ente", "cod_imposta", "cod_tipoimposta"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            if df[col].isna().any():
                median_val = df[col].median()
                filled = int(df[col].isna().sum())
                df[col] = df[col].fillna(median_val)
                changes.append(f"Filled {filled} nulls in '{col}' with median ({median_val:.2f})")

    # 4. Fill remaining string columns with 'Unknown'
    string_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in string_cols:
        null_count = int(df[col].isna().sum())
        if null_count > 0:
            df[col] = df[col].fillna("Unknown")
            changes.append(f"Filled {null_count} nulls in '{col}' with 'Unknown'")

    df.to_csv(output_path, index=False)
    return {"fixes_applied": changes, "output_path": output_path}
