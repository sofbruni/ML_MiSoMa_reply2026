"""
Remediation tools:
  - generate_correction_suggestions : LLM-callable tool that formulates a fix per issue
  - calculate_reliability_score     : computes a 0-100 score from all findings
  - build_final_report              : (non-tool) assembles the complete quality report dict
"""

import json
import os
import pandas as pd
from langchain_core.tools import tool


@tool
def generate_correction_suggestions(findings_json: str) -> str:
    """Given a JSON string of all data-quality findings, produce a list of
    actionable correction suggestions (one per issue). Returns a JSON string."""
    try:
        findings = json.loads(findings_json)
    except json.JSONDecodeError:
        return json.dumps({"error": "Invalid JSON input"})

    suggestions = []

    # Schema
    schema = findings.get("schema", {})
    for col, msgs in schema.get("type_issues", {}).items():
        suggestions.append({
            "field": col, "issue": msgs,
            "action": f"Coerce '{col}' to its expected numeric/date type; replace unparseable values with NaN"
        })
    for col, reason in schema.get("duplicate_columns", {}).items():
        suggestions.append({
            "field": col, "issue": reason,
            "action": f"Drop redundant column '{col}'"
        })
    for col, problems in schema.get("naming_issues", {}).items():
        suggestions.append({
            "field": col, "issue": problems,
            "action": f"Rename '{col}' to a lowercase snake_case equivalent"
        })

    # Completeness
    completeness = findings.get("completeness", {})
    for col, info in completeness.get("missing_per_column", {}).items():
        pct = info.get("null_pct", 0)
        if pct > 95:
            suggestions.append({
                "field": col, "issue": f"{pct}% missing",
                "action": f"Consider dropping column '{col}' — nearly entirely empty"
            })
        elif pct > 20:
            suggestions.append({
                "field": col, "issue": f"{pct}% missing",
                "action": f"Flag rows with missing '{col}'; fill with median (numeric) or 'Unknown' (categorical)"
            })
        else:
            suggestions.append({
                "field": col, "issue": f"{pct}% missing",
                "action": f"Fill nulls in '{col}' with median or mode"
            })

    # Consistency
    consistency = findings.get("consistency", {})
    for col, info in consistency.get("format_issues", {}).items():
        suggestions.append({
            "field": col, "issue": info.get("problem", "format inconsistency"),
            "action": f"Standardise all values in '{col}' to a single canonical format"
        })
    for key, info in consistency.get("cross_column_issues", {}).items():
        suggestions.append({
            "field": key, "issue": info.get("problem", "cross-column mismatch"),
            "action": "Reconcile the conflicting columns using the authoritative source"
        })
    dup = consistency.get("duplicate_issues", {})
    if dup.get("exact_duplicates", {}).get("count", 0) > 0:
        suggestions.append({
            "field": "all_columns", "issue": f"{dup['exact_duplicates']['count']} exact duplicates",
            "action": "Remove exact duplicate rows"
        })

    # Anomaly
    anomaly = findings.get("anomaly", {})
    for col, info in anomaly.get("numerical_outliers", {}).items():
        n_iqr = info.get("iqr_outliers", {}).get("count", 0)
        if n_iqr > 0:
            suggestions.append({
                "field": col, "issue": f"{n_iqr} IQR outliers detected",
                "action": f"Investigate high '{col}' values; cap at 99th percentile or flag for manual review"
            })
    for col, info in anomaly.get("categorical_anomalies", {}).items():
        rare = info.get("rare_values", [])
        if rare:
            suggestions.append({
                "field": col, "issue": f"Rare categories: {rare[:3]}",
                "action": f"Map rare values in '{col}' to 'Other' or investigate their validity"
            })

    return json.dumps({"correction_suggestions": suggestions}, indent=2)


@tool
def calculate_reliability_score(findings_json: str) -> str:
    """Compute a 0–100 reliability score from the aggregated findings.
    Per-column deductions are scaled by dataset width so wide datasets are not
    over-penalised relative to narrow ones. Returns JSON with the score and breakdown."""
    try:
        findings = json.loads(findings_json)
    except json.JSONDecodeError:
        return json.dumps({"error": "Invalid JSON input"})

    score = 100.0
    deductions = []

    # Scale per-column deductions for wide datasets (baseline = 10 columns)
    column_count = findings.get("column_count", 10)
    col_scale = min(10 / max(column_count, 1), 1.0)

    def _count(val) -> int:
        """Accept either a dict/list (return len) or an int/float (return as int)."""
        if isinstance(val, (dict, list)):
            return len(val)
        if isinstance(val, (int, float)):
            return int(val)
        return 0

    # Schema deductions (per-column → scaled)
    schema = findings.get("schema", {})
    n_type_issues = _count(schema.get("type_issues", {}))
    n_naming = _count(schema.get("naming_issues", {})) + _count(schema.get("duplicate_columns", {}))
    if n_type_issues:
        d = round(min(n_type_issues * 3 * col_scale, 15), 1)
        score -= d
        deductions.append(f"-{d} pts: {n_type_issues} data-type issue(s)")
    if n_naming:
        d = round(min(n_naming * 1 * col_scale, 5), 1)
        score -= d
        deductions.append(f"-{d} pts: {n_naming} naming/duplicate column issue(s)")

    # Completeness deductions (global → not scaled)
    completeness = findings.get("completeness", {})
    overall_pct = completeness.get("overall_completeness_pct", 100)
    missing_penalty = round((100 - overall_pct) * 0.5, 1)
    if missing_penalty > 0:
        score -= missing_penalty
        deductions.append(f"-{missing_penalty} pts: overall completeness {overall_pct}%")

    # Consistency deductions (format issues scaled; duplicates global)
    consistency = findings.get("consistency", {})
    n_fmt = _count(consistency.get("format_issues", {}))
    n_cross = _count(consistency.get("cross_column_issues", {}))
    n_dupes = consistency.get("duplicate_issues", {}).get("exact_duplicates", {}).get("count", 0)
    if n_fmt:
        d = round(min(n_fmt * 3 * col_scale, 10), 1)
        score -= d
        deductions.append(f"-{d} pts: {n_fmt} format inconsistency issue(s)")
    if n_cross:
        score -= 5
        deductions.append("-5 pts: cross-column logic violations")
    if n_dupes:
        d = min(round(n_dupes / 100, 1), 10)
        score -= d
        deductions.append(f"-{d} pts: {n_dupes} duplicate rows")

    # Anomaly deductions (global → not scaled)
    anomaly = findings.get("anomaly", {})
    for col, info in anomaly.get("numerical_outliers", {}).items():
        n_out = info.get("iqr_outliers", {}).get("count", 0)
        if n_out:
            d = min(round(n_out / 50, 1), 10)
            score -= d
            deductions.append(f"-{d} pts: {n_out} outliers in '{col}'")

    score = max(0.0, round(score, 1))
    grade = "A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60 else "D" if score >= 45 else "F"

    return json.dumps({
        "reliability_score": score,
        "grade": grade,
        "deductions": deductions,
        "interpretation": f"Dataset reliability: {grade} ({score}/100)",
    }, indent=2)


# ---------------------------------------------------------------------------
# Non-tool helper: assemble the final report
# ---------------------------------------------------------------------------

def build_final_report(
    original_path: str,
    fixed_path: str,
    schema_findings: dict,
    completeness_findings: dict,
    consistency_findings: dict,
    anomaly_findings: dict,
    suggestions: list,
    score_result: dict,
) -> dict:
    """Combine all findings into a single quality report dict."""
    orig_df = pd.read_csv(original_path, dtype=str)
    fixed_df = pd.read_csv(fixed_path, dtype=str)

    return {
        "summary": {
            "original_path": original_path,
            "fixed_path": fixed_path,
            "original_rows": len(orig_df),
            "fixed_rows": len(fixed_df),
            "original_columns": len(orig_df.columns),
            "fixed_columns": len(fixed_df.columns),
            "reliability_score": score_result.get("reliability_score"),
            "grade": score_result.get("grade"),
        },
        "schema": schema_findings,
        "completeness": completeness_findings,
        "consistency": consistency_findings,
        "anomaly": anomaly_findings,
        "correction_suggestions": suggestions,
        "score_breakdown": score_result.get("deductions", []),
    }
