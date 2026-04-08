"""
Remediation tools:
  - generate_correction_suggestions : LLM-callable tool that formulates fixes with confidence scoring
  - calculate_reliability_score     : computes a 0-100 score plus prioritized roadmap
  - apply_remediation_fixes         : applies high-confidence low-risk fixes directly to CSV
  - build_final_report              : (non-tool) assembles the complete quality report dict
"""

from __future__ import annotations

import json
import re
from typing import Any

import pandas as pd
from langchain_core.tools import tool


def _load_payload(findings_json: str) -> dict[str, Any]:
    try:
        payload = json.loads(findings_json)
    except json.JSONDecodeError:
        return {"error": "Invalid JSON input"}

    if isinstance(payload, dict):
        return payload
    return {"raw": payload}


def _extract_findings_text(payload: dict[str, Any]) -> str:
    if "all_findings_text" in payload:
        return str(payload.get("all_findings_text") or "")
    return json.dumps(payload, ensure_ascii=False)


def _count_from_text(text: str, patterns: list[str]) -> int:
    total = 0
    for pat in patterns:
        for m in re.findall(pat, text, flags=re.IGNORECASE):
            if isinstance(m, tuple):
                m = m[0]
            try:
                total += int(float(m))
            except (TypeError, ValueError):
                continue
    return total


def _safe_value(raw: Any, default: float = 0.0) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _build_suggestion(
    field: str,
    issue: str,
    action: str,
    confidence: float,
    risk: str,
) -> dict[str, Any]:
    return {
        "field": field,
        "issue": issue,
        "action": action,
        "confidence": round(confidence, 2),
        "risk": risk,
        "auto_apply": confidence >= 0.9 and risk == "low",
    }


@tool
def generate_correction_suggestions(findings_json: str) -> str:
    """Generate correction suggestions with confidence/risk metadata.

    This function supports both structured findings and free-text aggregated findings.
    """
    payload = _load_payload(findings_json)
    if "error" in payload:
        return json.dumps(payload)

    text = _extract_findings_text(payload)
    suggestions: list[dict[str, Any]] = []

    # Deterministic baseline suggestions that are generally safe.
    suggestions.append(
        _build_suggestion(
            "all_text_columns",
            "Potential whitespace / casing inconsistency",
            "Trim leading/trailing whitespace across string columns",
            0.97,
            "low",
        )
    )

    if re.search(r"hyphen|snake_case|uppercase|naming", text, flags=re.IGNORECASE):
        suggestions.append(
            _build_suggestion(
                "column_names",
                "Naming convention violations detected",
                "Normalize column names to lowercase snake_case",
                0.95,
                "low",
            )
        )

    duplicate_rows = _count_from_text(text, [r"Removed\s+(\d+)\s+exact duplicate row", r"(\d+)\s+exact duplicates"])
    if duplicate_rows > 0:
        suggestions.append(
            _build_suggestion(
                "rows",
                f"{duplicate_rows} duplicate rows",
                "Remove exact duplicate rows",
                0.9,
                "low",
            )
        )

    if re.search(r"outlier|rare categor", text, flags=re.IGNORECASE):
        suggestions.append(
            _build_suggestion(
                "anomaly_columns",
                "Outliers or rare values flagged",
                "Cap extreme numeric values at 99th percentile and map very rare categories to 'Other'",
                0.82,
                "medium",
            )
        )

    missing_pct = _count_from_text(text, [r"(\d+(?:\.\d+)?)%\s+missing"])  # rough signal
    if missing_pct > 0 or re.search(r"null|missing|sparse", text, flags=re.IGNORECASE):
        suggestions.append(
            _build_suggestion(
                "missing_values",
                "Missingness detected",
                "Preserve nulls as NaN; add missingness indicators and require manual/domain-specific review",
                0.92,
                "low",
            )
        )

    if not suggestions:
        suggestions.append(
            _build_suggestion(
                "dataset",
                "No major issue patterns detected from findings text",
                "Run targeted manual review for domain-specific logic checks",
                0.4,
                "high",
            )
        )

    return json.dumps({"correction_suggestions": suggestions}, indent=2, ensure_ascii=False)


def _apply_single_fix(df: pd.DataFrame, suggestion: dict[str, Any]) -> tuple[pd.DataFrame, str]:
    action = str(suggestion.get("action", "")).lower()

    if "trim leading/trailing whitespace" in action:
        object_cols = df.select_dtypes(include=["object", "string"]).columns
        for col in object_cols:
            df[col] = df[col].astype(str).str.strip().replace("<NA>", pd.NA)
        return df, "Trimmed whitespace in string columns"

    if "normalize column names" in action:
        renamed = {
            c: re.sub(r"[^a-z0-9]+", "_", c.strip().lower()).strip("_") for c in df.columns
        }
        df = df.rename(columns=renamed)
        return df, "Normalized column names to snake_case"

    if "remove exact duplicate rows" in action:
        before = len(df)
        df = df.drop_duplicates()
        return df, f"Removed {before - len(df)} exact duplicate rows"

    if "preserve nulls as nan" in action:
        return df, "Preserved missing values as NaN (no imputation)"

    return df, f"Skipped unsupported auto-fix action: {suggestion.get('action', 'unknown')}"


def apply_remediation_fixes(input_path: str, output_path: str, suggestions: list[dict[str, Any]]) -> dict[str, Any]:
    """Apply safe high-confidence fixes and log lower-confidence actions."""
    df = pd.read_csv(input_path, dtype=str)

    applied: list[str] = []
    skipped: list[str] = []

    for suggestion in suggestions:
        confidence = _safe_value(suggestion.get("confidence"), 0.0)
        risk = str(suggestion.get("risk", "high")).lower()
        if confidence >= 0.9 and risk == "low":
            df, message = _apply_single_fix(df, suggestion)
            applied.append(message)
        else:
            skipped.append(
                f"{suggestion.get('action', 'unknown action')} (confidence: {confidence:.2f}, risk: {risk})"
            )

    df.to_csv(output_path, index=False, encoding="utf-8")

    return {
        "applied_fixes": applied,
        "skipped_fixes": skipped,
        "fixes_applied_count": len(applied),
        "manual_review_required": len(skipped) > 0,
    }


def _generate_improvement_roadmap_from_text(text: str) -> list[dict[str, Any]]:
    roadmap: list[dict[str, Any]] = []

    if re.search(r"duplicate", text, flags=re.IGNORECASE):
        roadmap.append(
            {
                "priority": 1,
                "category": "Consistency",
                "action": "Remove duplicate rows and reconcile near-duplicates",
                "score_impact": 8.0,
                "difficulty": "easy",
                "auto_applicable": True,
            }
        )

    if re.search(r"missing|null|sparse", text, flags=re.IGNORECASE):
        roadmap.append(
            {
                "priority": len(roadmap) + 1,
                "category": "Completeness",
                "action": "Impute or drop highly sparse fields",
                "score_impact": 7.0,
                "difficulty": "medium",
                "auto_applicable": True,
            }
        )

    if re.search(r"type|format|date", text, flags=re.IGNORECASE):
        roadmap.append(
            {
                "priority": len(roadmap) + 1,
                "category": "Schema",
                "action": "Standardize types and date formats",
                "score_impact": 6.0,
                "difficulty": "easy",
                "auto_applicable": True,
            }
        )

    if re.search(r"outlier|rare", text, flags=re.IGNORECASE):
        roadmap.append(
            {
                "priority": len(roadmap) + 1,
                "category": "Anomaly",
                "action": "Review outliers and rare categories",
                "score_impact": 4.0,
                "difficulty": "medium",
                "auto_applicable": False,
            }
        )

    if not roadmap:
        roadmap = [
            {
                "priority": 1,
                "category": "General",
                "action": "Perform manual spot-check on key business columns",
                "score_impact": 3.0,
                "difficulty": "medium",
                "auto_applicable": False,
            }
        ]

    roadmap.sort(key=lambda x: x["score_impact"], reverse=True)
    for idx, item in enumerate(roadmap, start=1):
        item["priority"] = idx
    return roadmap


@tool
def calculate_reliability_score(findings_json: str) -> str:
    """Compute a 0-100 reliability score with deductions and improvement roadmap."""
    payload = _load_payload(findings_json)
    if "error" in payload:
        return json.dumps(payload)

    text = _extract_findings_text(payload)
    column_count = max(int(payload.get("column_count", 10) or 10), 1)

    deductions: list[str] = []
    score = 100.0
    col_scale = min(10 / column_count, 1.0)

    missing_pct_matches = re.findall(r"(\d+(?:\.\d+)?)%\s+missing", text, flags=re.IGNORECASE)
    if missing_pct_matches:
        avg_missing = sum(float(x) for x in missing_pct_matches) / len(missing_pct_matches)
        d = round(min(avg_missing * 0.4, 20), 1)
        score -= d
        deductions.append(f"-{d} pts: inferred missingness from findings")

    naming_hits = len(re.findall(r"snake_case|uppercase|hyphen|naming", text, flags=re.IGNORECASE))
    if naming_hits:
        d = round(min(naming_hits * 1.0 * col_scale, 5), 1)
        score -= d
        deductions.append(f"-{d} pts: naming/schema inconsistencies")

    duplicate_rows = _count_from_text(text, [r"Removed\s+(\d+)\s+exact duplicate row", r"(\d+)\s+exact duplicates"])
    if duplicate_rows:
        d = round(min(duplicate_rows / 100, 10), 1)
        score -= d
        deductions.append(f"-{d} pts: {duplicate_rows} duplicate rows")

    outlier_count = _count_from_text(text, [r"(\d+)\s+IQR outliers", r"(\d+)\s+outliers"])
    if outlier_count:
        d = round(min(outlier_count / 50, 10), 1)
        score -= d
        deductions.append(f"-{d} pts: inferred outlier volume")

    if re.search(r"cross-column mismatch|inconsistent with", text, flags=re.IGNORECASE):
        score -= 5
        deductions.append("-5 pts: cross-column consistency issues")

    score = max(0.0, round(score, 1))
    grade = "A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60 else "D" if score >= 45 else "F"

    roadmap = _generate_improvement_roadmap_from_text(text)
    expected_gain = sum(item.get("score_impact", 0.0) for item in roadmap[:3])
    expected_score_after_fixes = min(100.0, round(score + expected_gain, 1))

    return json.dumps(
        {
            "reliability_score": score,
            "grade": grade,
            "deductions": deductions,
            "improvement_roadmap": roadmap,
            "expected_score_after_fixes": expected_score_after_fixes,
            "interpretation": f"Dataset reliability: {grade} ({score}/100)",
        },
        indent=2,
        ensure_ascii=False,
    )


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
        "improvement_roadmap": score_result.get("improvement_roadmap", []),
        "expected_score_after_fixes": score_result.get("expected_score_after_fixes"),
    }
