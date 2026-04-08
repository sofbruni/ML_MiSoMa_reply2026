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
    return df.replace(list(PLACEHOLDER_VALUES), pd.NA).infer_objects(copy=False)


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

def apply_completeness_fixes(input_path: str, output_path: str, profile: dict) -> dict:
    """
    Deterministic completeness fixes:
      1. Replace known placeholder strings with NaN.
      2. Drop columns that are >95% empty (ultra-sparse).
      3. Preserve remaining missing values as NaN (no imputation).
    """
    df = pd.read_csv(input_path, dtype=str)
    n = len(df)
    changes = []

    # 1. Placeholder → NaN
    before_nulls = int(df.isna().sum().sum())
    df = df.replace(list(PLACEHOLDER_VALUES), pd.NA).infer_objects(copy=False)
    after_nulls = int(df.isna().sum().sum())
    if after_nulls > before_nulls:
        changes.append(f"Converted {after_nulls - before_nulls} placeholder strings to NaN")

    # 2. Drop ultra-sparse columns (>95% missing)
    ultra_sparse = [c for c in df.columns if df[c].isna().sum() / n > 0.95]
    if ultra_sparse:
        df = df.drop(columns=ultra_sparse)
        changes.append(f"Dropped ultra-sparse columns (>95% empty): {ultra_sparse}")

    # 3. Preserve NaNs by design (no auto-imputation).
    remaining_nans = int(df.isna().sum().sum())
    if remaining_nans > 0:
        changes.append(f"Preserved {remaining_nans} missing value(s) as NaN (no imputation policy)")

    # 4. Coerce whole-number numeric columns to Int64 (avoids .0 suffix in CSV output).
    for col in df.select_dtypes(include=["float64", "Float64"]).columns:
        series = df[col]
        non_null = series.dropna()
        if len(non_null) > 0 and (non_null % 1 == 0).all():
            df[col] = series.apply(lambda x: int(x) if pd.notna(x) else pd.NA).astype("Int64")

    df.to_csv(output_path, index=False, encoding="utf-8")
    return {"fixes_applied": changes, "output_path": output_path}
