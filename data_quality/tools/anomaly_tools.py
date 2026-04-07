"""
Anomaly Detection tools:
  - detect_numerical_outliers     : IQR + Z-score on ALL numeric columns
  - detect_categorical_anomalies  : rare values in ALL low-cardinality string columns
"""

import json
import math
import pandas as pd
from langchain_core.tools import tool


def _safe_json(obj, **kwargs) -> str:
    """json.dumps that replaces float NaN/Inf with None (valid JSON null)."""
    def _sanitize(o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        if isinstance(o, dict):
            return {k: _sanitize(v) for k, v in o.items()}
        if isinstance(o, list):
            return [_sanitize(v) for v in o]
        return o
    return json.dumps(_sanitize(obj), **kwargs)


@tool
def detect_numerical_outliers(dataset_path: str) -> str:
    """Apply IQR and Z-score outlier detection to ALL numeric columns in the dataset.
    A column is treated as numeric if >90% of non-null values parse as float.
    Skips columns with fewer than 10 non-null values.
    Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    findings: dict = {}
    NUMERIC_THRESHOLD = 0.9

    for col in df.columns:
        series_raw = df[col].dropna()
        if len(series_raw) < 10:
            continue
        numeric = pd.to_numeric(series_raw, errors="coerce")
        valid_ratio = numeric.notna().sum() / len(series_raw)
        if valid_ratio < NUMERIC_THRESHOLD:
            continue

        series = numeric.dropna()
        n = len(series)

        # IQR method
        q1 = float(series.quantile(0.25))
        q3 = float(series.quantile(0.75))
        iqr = q3 - q1
        lower_iqr = q1 - 1.5 * iqr
        upper_iqr = q3 + 1.5 * iqr
        iqr_outliers = series[(series < lower_iqr) | (series > upper_iqr)]

        # Z-score method
        mean = float(series.mean())
        std = float(series.std())
        z_scores = (series - mean) / std if std > 0 else pd.Series([0.0] * n, index=series.index)
        z_outliers = series[z_scores.abs() > 3]

        if len(iqr_outliers) > 0 or len(z_outliers) > 0:
            findings[col] = {
                "n_values": n,
                "mean": round(mean, 2),
                "std": round(std, 2),
                "min": round(float(series.min()), 2),
                "max": round(float(series.max()), 2),
                "iqr_outliers": {
                    "count": len(iqr_outliers),
                    "lower_fence": round(lower_iqr, 2),
                    "upper_fence": round(upper_iqr, 2),
                    "examples": iqr_outliers.nlargest(5).round(2).tolist(),
                },
                "zscore_outliers": {
                    "count": len(z_outliers),
                    "threshold": 3,
                    "examples": z_outliers.nlargest(5).round(2).tolist(),
                },
            }

    return _safe_json({
        "check": "univariate_outlier_detection",
        "findings": findings,
        "status": "issues_found" if findings else "ok",
    }, indent=2)


@tool
def detect_categorical_anomalies(dataset_path: str) -> str:
    """Flag rare or invalid categorical values in ALL low-cardinality string columns.
    A column is treated as categorical if it has ≤50 unique non-null values and
    is not predominantly numeric. A value is 'rare' if it appears in <0.5% of rows.
    Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    n = len(df)
    findings: dict = {}

    RARE_THRESHOLD = 0.005    # 0.5%
    MAX_CARDINALITY = 50
    NUMERIC_THRESHOLD = 0.9
    INVALID_MARKERS = {"Unknown", "n.d.", "N/A", "?", "//", "-", "ND"}

    for col in df.columns:
        series = df[col].dropna()
        if len(series) == 0:
            continue
        # Skip predominantly numeric columns
        numeric_ratio = pd.to_numeric(series, errors="coerce").notna().sum() / len(series)
        if numeric_ratio >= NUMERIC_THRESHOLD:
            continue
        # Skip high-cardinality (free-text) columns
        if series.nunique() > MAX_CARDINALITY:
            continue

        col_findings: dict = {}

        # Rare value detection
        vc = df[col].value_counts(dropna=False)
        rare = vc[vc / n < RARE_THRESHOLD]
        if len(rare) > 0:
            col_findings["rare_values"] = rare.index.tolist()[:10]
            col_findings["rare_counts"] = rare.values.tolist()[:10]
            col_findings["total_unique"] = int(df[col].nunique())
            col_findings["rare_threshold_pct"] = RARE_THRESHOLD * 100

        # Remaining invalid markers
        invalid_present = df[col][df[col].isin(INVALID_MARKERS)].value_counts().to_dict()
        if invalid_present:
            col_findings["remaining_invalid_markers"] = invalid_present

        if col_findings:
            findings[col] = col_findings

    return _safe_json({
        "check": "categorical_anomaly_detection",
        "findings": findings,
        "status": "issues_found" if findings else "ok",
    }, indent=2)
