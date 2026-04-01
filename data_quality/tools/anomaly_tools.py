"""
Anomaly Detection tools:
  - detect_numerical_outliers     : IQR + Z-score on spesa column
  - detect_categorical_anomalies  : rare values in categorical columns
"""

import json
import numpy as np
import pandas as pd
from langchain_core.tools import tool


@tool
def detect_numerical_outliers(dataset_path: str) -> str:
    """Apply IQR and Z-score methods to detect outliers in the 'spesa' (spending) column.
    Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    findings: dict = {}

    if "spesa" not in df.columns:
        return json.dumps({"check": "univariate_outlier_detection", "status": "skipped",
                           "reason": "column 'spesa' not found"})

    series = pd.to_numeric(df["spesa"], errors="coerce").dropna()
    n = len(series)

    # --- IQR method ---
    q1 = float(series.quantile(0.25))
    q3 = float(series.quantile(0.75))
    iqr = q3 - q1
    lower_iqr = q1 - 1.5 * iqr
    upper_iqr = q3 + 1.5 * iqr
    iqr_outliers = series[(series < lower_iqr) | (series > upper_iqr)]

    # --- Z-score method ---
    mean = float(series.mean())
    std = float(series.std())
    z_scores = (series - mean) / std if std > 0 else pd.Series([0.0] * n)
    z_outliers = series[z_scores.abs() > 3]

    findings["spesa"] = {
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

    return json.dumps({
        "check": "univariate_outlier_detection",
        "findings": findings,
        "status": "issues_found" if (len(iqr_outliers) > 0 or len(z_outliers) > 0) else "ok",
    }, indent=2)


@tool
def detect_categorical_anomalies(dataset_path: str) -> str:
    """Flag unexpected or rare categorical values in imposta, tipo_imposta,
    and area_geografica columns. A category is 'rare' if it appears in <0.5% of rows.
    Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    n = len(df)
    findings: dict = {}

    CATEGORICAL_COLS = ["imposta", "tipo_imposta", "area_geografica", "descrizione"]
    RARE_THRESHOLD = 0.005  # 0.5%

    for col in CATEGORICAL_COLS:
        if col not in df.columns:
            continue
        vc = df[col].value_counts(dropna=False)
        rare = vc[vc / n < RARE_THRESHOLD]
        if len(rare) > 0:
            findings[col] = {
                "rare_values": rare.index.tolist()[:10],
                "rare_counts": rare.values.tolist()[:10],
                "total_unique": int(df[col].nunique()),
                "rare_threshold_pct": RARE_THRESHOLD * 100,
            }

    # Also flag completely unknown/invalid values left after previous cleaning
    invalid_markers = {"Unknown", "n.d.", "N/A", "?", "//", "-", "ND"}
    for col in CATEGORICAL_COLS:
        if col not in df.columns:
            continue
        invalid_present = df[col][df[col].isin(invalid_markers)].value_counts().to_dict()
        if invalid_present:
            existing = findings.setdefault(col, {})
            existing["remaining_invalid_markers"] = invalid_present

    return json.dumps({
        "check": "categorical_anomaly_detection",
        "findings": findings,
        "status": "issues_found" if findings else "ok",
    }, indent=2)
