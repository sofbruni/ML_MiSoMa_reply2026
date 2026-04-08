"""Report generation helpers for data quality outputs."""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

from data_quality.config import get_llm
from data_quality.orchestration.helpers import extract_text

llm = get_llm()

_TEAM_META = {
    "schema_team": {
        "title": "Schema Validation",
        "desc": "Data types, naming conventions, duplicate columns.",
    },
    "completeness_team": {
        "title": "Completeness Analysis",
        "desc": "Missing values, null rates, sparse columns.",
    },
    "consistency_team": {
        "title": "Consistency Validation",
        "desc": "Format normalization, duplicate rows, logic checks.",
    },
    "anomaly_team": {
        "title": "Anomaly Detection",
        "desc": "Outliers and rare-category analysis.",
    },
    "remediation_team": {
        "title": "Remediation and Reliability",
        "desc": "Active fixes, confidence scoring, final score.",
    },
}

_SKIP_PREFIXES = (
    "[schema team]",
    "[completeness team]",
    "[consistency team]",
    "[anomaly team]",
    "[remediation team]",
    "fixes applied:",
    "fixed csv:",
    "final cleaned dataset:",
)


def clean_findings(raw: str) -> str:
    """Remove repetitive technical prefixes from team findings."""
    lines = [
        line for line in raw.splitlines()
        if not any(line.lower().strip().startswith(prefix) for prefix in _SKIP_PREFIXES)
    ]
    return "\n".join(lines).strip()


def extract_score(text: str) -> tuple[str, str]:
    """Extract score and grade from remediation narrative text."""
    match = re.search(r"(?:score|punteggio)[^\d]*(\d{1,3}(?:\.\d+)?)\s*/\s*100", text, re.IGNORECASE)
    if not match:
        match = re.search(r"\b(\d{1,3}(?:\.\d+)?)\s*/\s*100\b", text)
    score = match.group(1) if match else ""

    grade_match = re.search(r"\bgrade[:\s]+([A-F][+-]?)\b", text, re.IGNORECASE)
    grade = grade_match.group(1) if grade_match else ""
    return score, grade


def build_markdown(report: dict) -> str:
    """Build the markdown quality report shown to users."""
    dataset_name = Path(report.get("original_path", "unknown")).name
    today = date.today().strftime("%B %d, %Y")
    teams_executed = report.get("teams_executed", [])
    findings = report.get("findings", {})
    score, grade = extract_score(findings.get("remediation_team", ""))

    lines: list[str] = [
        "# Data Quality Report",
        "",
        "| | |",
        "|---|---|",
        f"| **Dataset** | `{dataset_name}` |",
        f"| **Date** | {today} |",
        f"| **Teams executed** | {len(teams_executed)} |",
        f"| **Iterations** | {report.get('iteration_count', 0)} |",
        f"| **Teams skipped** | {', '.join(report.get('teams_skipped', [])) or 'none'} |",
    ]
    if score:
        grade_part = f"  -  Grade **{grade}**" if grade else ""
        lines.append(f"| **Reliability score** | **{score}/100**{grade_part} |")
    lines += ["", "---", ""]

    narrative = report.get("supervisor_narrative", "")
    if narrative:
        lines += ["## Supervisor Narrative", "", f"> {narrative}", "", "---", ""]

    decisions = report.get("supervisor_decisions", [])
    if decisions:
        lines += ["## Supervisor Decisions", ""]
        lines += ["| Step | Selected | Source | Confidence | Reason |", "|---|---|---|---|---|"]
        for decision in decisions:
            reason = str(decision.get("reason", "")).replace("|", "/")
            lines.append(
                f"| {decision.get('step', '')} | {decision.get('selected', '')} | "
                f"{decision.get('source', '')} | {decision.get('confidence', '')} | {reason} |"
            )
        lines += ["", "---", ""]

    lines += ["## Team Outputs", ""]
    for key in ["schema_team", "completeness_team", "consistency_team", "anomaly_team", "remediation_team"]:
        if key not in findings:
            continue
        meta = _TEAM_META[key]
        lines += [f"### {meta['title']}", "", f"> {meta['desc']}", "", clean_findings(findings[key]), ""]

    return "\n".join(lines)


def default_supervisor_narrative(report: dict) -> str:
    """Fallback narrative if LLM narrative generation fails."""
    executed = report.get("teams_executed", [])
    skipped = report.get("teams_skipped", [])
    iterations = report.get("iteration_count", 0)
    llm_steps = len([d for d in report.get("supervisor_decisions", []) if d.get("source") == "llm"])
    return (
        f"Pipeline executed {len(executed)} teams with {iterations} iteration(s). "
        f"Supervisor skipped: {', '.join(skipped) if skipped else 'none'}. "
        f"LLM-guided routing was used in {llm_steps} decision(s), while deterministic guardrails "
        "enforced safe ordering and termination."
    )


def generate_supervisor_narrative(report: dict) -> str:
    """Create a short slide-friendly narrative for supervisor behavior."""
    context = {
        "teams_executed": report.get("teams_executed", []),
        "teams_skipped": report.get("teams_skipped", []),
        "iteration_count": report.get("iteration_count", 0),
        "rows_removed_total": report.get("rows_removed_total", 0),
        "supervisor_decisions": report.get("supervisor_decisions", []),
    }
    prompt = (
        "Write a concise executive narrative (max 90 words) for presentation slides.\n"
        "Explain how the hybrid supervisor made routing decisions using both deterministic guardrails "
        "and LLM judgment.\n"
        "Mention executed vs skipped teams and whether iteration happened.\n"
        "Do not use bullet points.\n\n"
        f"Context JSON:\n{json.dumps(context, ensure_ascii=False)}"
    )
    try:
        ai_msg = llm.invoke([{"role": "user", "content": prompt}])
        text = extract_text(ai_msg.content).strip()
        if text:
            return re.sub(r"\s+", " ", text)
    except Exception:
        pass
    return default_supervisor_narrative(report)
