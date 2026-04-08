"""
Dataset Profiler — runs once before the pipeline to detect column semantic types.
Pure pandas heuristics, no LLM calls.

Returns a DatasetProfile dict:
  {
    "row_count": int,
    "column_count": int,
    "columns": {
      "<col_name>": {
        "dtype": str,
        "semantic_type": "numeric"|"date"|"categorical"|"identifier"|"boolean"|"text",
        "null_pct": float,
        "cardinality": int,
        "cardinality_ratio": float,
        "sample_values": list[str],
      }
    }
  }
"""

import csv
import warnings
from pathlib import Path

import pandas as pd


def profile_dataset(csv_path: str) -> dict:
    """
    Read the CSV at csv_path and return a DatasetProfile dict.
    Auto-detects the CSV delimiter. Raises FileNotFoundError if path is missing.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {csv_path}")

    # Auto-detect delimiter from first 4 KB
    sep = _detect_separator(csv_path)

    df = pd.read_csv(csv_path, dtype=str, sep=sep, encoding="utf-8", encoding_errors="replace")
    n = len(df)

    columns: dict[str, dict] = {}
    for col in df.columns:
        series = df[col].dropna()
        total_non_null = len(series)
        null_count = int(df[col].isna().sum())
        null_pct = round(null_count / n * 100, 2) if n > 0 else 0.0
        cardinality = int(series.nunique())
        cardinality_ratio = round(cardinality / n, 4) if n > 0 else 0.0
        sample_values = [str(v) for v in series.unique()[:5].tolist()]
        dtype_str = str(df[col].dtype)

        semantic_type = _infer_semantic_type(series, cardinality, cardinality_ratio, total_non_null)

        columns[col] = {
            "dtype": dtype_str,
            "semantic_type": semantic_type,
            "null_pct": null_pct,
            "cardinality": cardinality,
            "cardinality_ratio": cardinality_ratio,
            "sample_values": sample_values,
        }

    return {
        "row_count": n,
        "column_count": len(df.columns),
        "columns": columns,
    }


def _detect_separator(csv_path: str) -> str:
    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            sample = f.read(4096)
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except csv.Error:
        return ","


def _infer_semantic_type(
    series: pd.Series,
    cardinality: int,
    cardinality_ratio: float,
    total_non_null: int,
) -> str:
    if total_non_null == 0:
        return "text"

    # 1. Boolean: ≤3 unique values, all in boolean vocabulary
    BOOL_SET = {"true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"}
    if cardinality <= 3:
        unique_lower = set(series.str.lower().unique())
        if unique_lower <= BOOL_SET:
            return "boolean"

    # 2. Identifier: nearly all values are unique
    if cardinality_ratio > 0.95 and cardinality > 10:
        return "identifier"

    # 3. Numeric: >90% of non-null values parse as float
    #    NOTE: YYYYMM codes (e.g. 202401) are 6-digit integers and will hit this branch,
    #    correctly preventing them from being classified as dates.
    numeric_ok = pd.to_numeric(series, errors="coerce").notna().sum()
    if numeric_ok / total_non_null >= 0.9:
        return "numeric"

    # 4. Date: >70% of non-null values parse as datetime (and not already numeric)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        date_ok = pd.to_datetime(series, errors="coerce").notna().sum()
    if date_ok / total_non_null >= 0.7:
        return "date"

    # 5. Categorical: low cardinality ratio
    if cardinality_ratio < 0.05:
        return "categorical"

    # 6. Fallback
    return "text"
