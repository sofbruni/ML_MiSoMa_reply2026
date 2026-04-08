"""Top-level hierarchical graph with smart routing and iteration support."""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any, Literal
from typing_extensions import TypedDict

from langchain_core.messages import HumanMessage
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

from data_quality.profiling import create_dataset_profile
from data_quality.state import DataQualityState
from data_quality.teams.anomaly_team import anomaly_graph
from data_quality.teams.completeness_team import completeness_graph
from data_quality.teams.consistency_team import consistency_graph
from data_quality.teams.remediation_team import remediation_graph
from data_quality.teams.schema_team import schema_graph
from data_quality.tools.completeness_tools import apply_completeness_fixes
from data_quality.tools.consistency_tools import apply_consistency_fixes
from data_quality.tools.remediation_tools import (
    apply_remediation_fixes,
    calculate_reliability_score,
    generate_correction_suggestions,
)
from data_quality.tools.schema_tools import apply_schema_fixes
from data_quality.tools.semantic_enricher import enrich_profile
from data_quality.config import get_llm

llm = get_llm()

TEAMS = ["schema_team", "completeness_team", "consistency_team", "anomaly_team", "remediation_team"]
LLM_SUPERVISOR_MIN_CONFIDENCE = 0.55


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _versioned_path(original_path: str, version: int, suffix: str = "") -> str:
    p = Path(original_path)
    suffix_part = f"_{suffix}" if suffix else ""
    return str(p.parent / f"{p.stem}_v{version}{suffix_part}{p.suffix}")


def _team_initial_message(working_path: str, task: str, profile: dict | None = None) -> list[dict]:
    content = f"{task}\n\nDataset path: {working_path}"
    if profile:
        content += f"\n\nDataset profile:\n{json.dumps(profile, ensure_ascii=False)}"
    return [{"role": "user", "content": content}]


def _extract_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            part.get("text", "") for part in content if isinstance(part, dict) and part.get("type") == "text"
        )
    return str(content)


def _collect_team_summary(result: dict) -> str:
    worker_msgs = [m for m in result["messages"] if isinstance(m, HumanMessage) and m.name]
    if worker_msgs:
        return "\n\n---\n\n".join(_extract_text(m.content) for m in worker_msgs)
    return _extract_text(result["messages"][-1].content)


def _tool_output_to_dict(tool_output: Any) -> dict:
    if isinstance(tool_output, str):
        try:
            return json.loads(tool_output)
        except json.JSONDecodeError:
            return {"raw": tool_output}
    if isinstance(tool_output, dict):
        return tool_output
    return {"raw": str(tool_output)}


def _mark_completed(state: DataQualityState, team_name: str) -> list[str]:
    completed = list(state.get("completed_teams", []))
    if team_name not in completed:
        completed.append(team_name)
    return completed


def _profile_skips(profile: dict) -> list[str]:
    skips = []
    if float(profile.get("overall_completeness", 1.0)) > 0.95:
        skips.append("completeness_team")
    has_anomaly_targets = bool(profile.get("numeric_columns") or profile.get("categorical_columns"))
    if not has_anomaly_targets:
        skips.append("anomaly_team")
    if int(profile.get("total_columns", 0)) < 3 or int(profile.get("total_rows", 0)) < 10:
        skips.append("consistency_team")
    return skips


def _iteration_target(state: DataQualityState) -> str | None:
    if int(state.get("iteration_count", 0)) >= 2:
        return None

    last_team = state.get("last_completed_team", "")
    profile = state.get("dataset_profile", {})
    total_rows = max(int(profile.get("total_rows", 0)), 1)

    if last_team == "consistency":
        rows_removed = int(state.get("rows_removed_last_team", 0))
        if rows_removed > total_rows * 0.1:
            return "completeness_team"

    if last_team == "schema":
        types_changed = int(state.get("types_changed_count", 0))
        if types_changed > 5:
            return "consistency_team"

    return None


def _eligible_teams(state: DataQualityState) -> list[str]:
    profile = state.get("dataset_profile", {})
    completed = set(state.get("completed_teams", []))
    eligible: list[str] = []

    if "schema_team" not in completed:
        return ["schema_team"]

    if "completeness_team" not in completed and float(profile.get("overall_completeness", 1.0)) < 0.95:
        eligible.append("completeness_team")

    if (
        "consistency_team" not in completed
        and int(profile.get("total_columns", 0)) >= 3
        and int(profile.get("total_rows", 0)) >= 10
    ):
        eligible.append("consistency_team")

    if "anomaly_team" not in completed and (profile.get("numeric_columns") or profile.get("categorical_columns")):
        eligible.append("anomaly_team")

    if not eligible and "remediation_team" not in completed:
        eligible.append("remediation_team")

    if not eligible:
        return ["FINISH"]
    return eligible


def _recent_findings_digest(state: DataQualityState) -> dict[str, str]:
    """Collect short, recent team summaries for LLM supervisor context."""
    team_names = {"schema_team", "completeness_team", "consistency_team", "anomaly_team", "remediation_team"}
    digest: dict[str, str] = {}
    for m in state.get("messages", []):
        if isinstance(m, HumanMessage) and m.name in team_names:
            digest[m.name] = _extract_text(m.content)[:800]
    return digest


def _is_borderline_case(state: DataQualityState, eligible: list[str]) -> bool:
    profile = state.get("dataset_profile", {})
    completeness = float(profile.get("overall_completeness", 1.0))
    rows = int(profile.get("total_rows", 0))
    cols = int(profile.get("total_columns", 0))
    if 0.93 <= completeness <= 0.97:
        return True
    if 8 <= rows <= 20 or 2 <= cols <= 5:
        return True
    return len(eligible) > 1


class LLMRoute(TypedDict):
    next: Literal["schema_team", "completeness_team", "consistency_team", "anomaly_team", "remediation_team", "FINISH"]
    reason: str
    confidence: float
    expected_impact: str


def _llm_supervisor_choice(state: DataQualityState, eligible: list[str]) -> dict[str, Any] | None:
    """Use LLM to choose next team only within deterministic guardrails."""
    if not eligible or eligible == ["FINISH"]:
        return None

    profile = state.get("dataset_profile", {})
    context = {
        "eligible_teams": eligible,
        "completed_teams": state.get("completed_teams", []),
        "iteration_count": state.get("iteration_count", 0),
        "last_completed_team": state.get("last_completed_team", ""),
        "rows_removed_last_team": state.get("rows_removed_last_team", 0),
        "types_changed_count": state.get("types_changed_count", 0),
        "profile_summary": {
            "total_rows": profile.get("total_rows", 0),
            "total_columns": profile.get("total_columns", 0),
            "overall_completeness": profile.get("overall_completeness", 1.0),
            "numeric_columns_count": len(profile.get("numeric_columns", [])),
            "categorical_columns_count": len(profile.get("categorical_columns", [])),
            "date_columns_count": len(profile.get("date_columns", [])),
        },
        "recent_findings_digest": _recent_findings_digest(state),
    }

    system_prompt = (
        "You are an orchestration supervisor for a data-quality multi-agent system. "
        "Choose the single best next team from eligible_teams only. "
        "Do not invent team names. Prefer highest expected quality gain and risk reduction."
    )
    user_prompt = (
        "Routing context JSON:\n"
        f"{json.dumps(context, ensure_ascii=False)}\n\n"
        "Return structured output with next, reason, confidence (0-1), expected_impact."
    )

    try:
        route = llm.with_structured_output(LLMRoute).invoke(
            [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}]
        )
    except Exception:
        return None

    chosen = route.get("next", "")
    confidence = float(route.get("confidence", 0.0) or 0.0)
    if chosen not in eligible:
        return None
    if confidence < LLM_SUPERVISOR_MIN_CONFIDENCE:
        return None
    return route


def _append_supervisor_decision(
    state: DataQualityState,
    *,
    selected: str,
    source: str,
    reason: str,
    confidence: float,
    eligible: list[str],
) -> list[dict]:
    decisions = list(state.get("supervisor_decisions", []))
    decisions.append(
        {
            "step": len(decisions) + 1,
            "selected": selected,
            "source": source,
            "reason": reason,
            "confidence": round(confidence, 3),
            "eligible": eligible,
            "iteration_count": int(state.get("iteration_count", 0)),
        }
    )
    return decisions


def smart_supervisor_node(state: DataQualityState) -> Command[Literal[*TEAMS, "__end__"]]:  # type: ignore[valid-type]
    profile = state.get("dataset_profile", {})
    eligible = _eligible_teams(state)

    pending_iteration = _iteration_target(state)
    if pending_iteration:
        decisions = _append_supervisor_decision(
            state,
            selected=pending_iteration,
            source="deterministic_iteration_rule",
            reason="Iteration trigger satisfied after previous team.",
            confidence=1.0,
            eligible=[pending_iteration],
        )
        return Command(
            goto=pending_iteration,
            update={
                "next": pending_iteration,
                "iteration_count": int(state.get("iteration_count", 0)) + 1,
                "skipped_teams": _profile_skips(profile),
                "supervisor_decisions": decisions,
            },
        )

    if eligible == ["FINISH"]:
        decisions = _append_supervisor_decision(
            state,
            selected="FINISH",
            source="deterministic",
            reason="No remaining eligible teams.",
            confidence=1.0,
            eligible=eligible,
        )
        return Command(goto=END, update={"next": "FINISH", "skipped_teams": _profile_skips(profile), "supervisor_decisions": decisions})

    deterministic_choice = eligible[0]
    selected = deterministic_choice
    source = "deterministic"
    reason = "Top eligible team by deterministic policy."
    confidence = 1.0

    if _is_borderline_case(state, eligible):
        llm_route = _llm_supervisor_choice(state, eligible)
        if llm_route:
            selected = llm_route["next"]
            source = "llm"
            reason = llm_route.get("reason", reason)
            confidence = float(llm_route.get("confidence", 0.0) or 0.0)

    decisions = _append_supervisor_decision(
        state,
        selected=selected,
        source=source,
        reason=reason,
        confidence=confidence,
        eligible=eligible,
    )
    return Command(
        goto=selected,
        update={
            "next": selected,
            "skipped_teams": _profile_skips(profile),
            "supervisor_decisions": decisions,
        },
    )


# ---------------------------------------------------------------------------
# Markdown report builder
# ---------------------------------------------------------------------------

_TEAM_META = {
    "schema_team": {"title": "Schema Validation", "desc": "Data types, naming conventions, duplicate columns."},
    "completeness_team": {"title": "Completeness Analysis", "desc": "Missing values, null rates, sparse columns."},
    "consistency_team": {"title": "Consistency Validation", "desc": "Format normalization, duplicate rows, logic checks."},
    "anomaly_team": {"title": "Anomaly Detection", "desc": "Outliers and rare-category analysis."},
    "remediation_team": {"title": "Remediation and Reliability", "desc": "Active fixes, confidence scoring, final score."},
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


def _clean_findings(raw: str) -> str:
    lines = [l for l in raw.splitlines() if not any(l.lower().strip().startswith(p) for p in _SKIP_PREFIXES)]
    return "\n".join(lines).strip()


def _extract_score(text: str) -> tuple[str, str]:
    m = re.search(r"(?:score|punteggio)[^\d]*(\d{1,3}(?:\.\d+)?)\s*/\s*100", text, re.IGNORECASE)
    if not m:
        m = re.search(r"\b(\d{1,3}(?:\.\d+)?)\s*/\s*100\b", text)
    score = m.group(1) if m else ""
    m2 = re.search(r"\bgrade[:\s]+([A-F][+-]?)\b", text, re.IGNORECASE)
    grade = m2.group(1) if m2 else ""
    return score, grade


def _build_markdown(report: dict) -> str:
    dataset_name = Path(report.get("original_path", "unknown")).name
    today = date.today().strftime("%B %d, %Y")
    teams_executed = report.get("teams_executed", [])
    findings = report.get("findings", {})
    score, grade = _extract_score(findings.get("remediation_team", ""))

    lines: list[str] = []
    lines += [
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
        lines += ["## Supervisor Narrative", ""]
        lines += [f"> {narrative}", "", "---", ""]

    decisions = report.get("supervisor_decisions", [])
    if decisions:
        lines += ["## Supervisor Decisions", ""]
        lines += ["| Step | Selected | Source | Confidence | Reason |", "|---|---|---|---|---|"]
        for d in decisions:
            reason = str(d.get("reason", "")).replace("|", "/")
            lines.append(
                f"| {d.get('step', '')} | {d.get('selected', '')} | {d.get('source', '')} | {d.get('confidence', '')} | {reason} |"
            )
        lines += ["", "---", ""]

    lines += ["## Team Outputs", ""]
    for key in ["schema_team", "completeness_team", "consistency_team", "anomaly_team", "remediation_team"]:
        if key not in findings:
            continue
        meta = _TEAM_META[key]
        lines += [f"### {meta['title']}", "", f"> {meta['desc']}", "", _clean_findings(findings[key]), ""]

    return "\n".join(lines)


def _default_supervisor_narrative(report: dict) -> str:
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


def _generate_supervisor_narrative(report: dict) -> str:
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
        text = _extract_text(ai_msg.content).strip()
        if text:
            return re.sub(r"\s+", " ", text)
    except Exception:
        pass
    return _default_supervisor_narrative(report)


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

def run_profiler(state: DataQualityState) -> dict:
    profile = create_dataset_profile(state["original_dataset_path"])
    return {
        "dataset_profile": profile,
        "completed_teams": [],
        "skipped_teams": [],
        "iteration_count": 0,
        "rows_removed": 0,
        "rows_removed_last_team": 0,
        "types_changed_count": 0,
        "last_completed_team": "",
        "supervisor_decisions": [],
    }


def run_semantic_enricher(state: DataQualityState) -> dict:
    prof = state["dataset_profile"]
    enrichments = enrich_profile(prof, llm)
    updated_profile = {**prof, "enrichments": enrichments}
    return {"dataset_profile": updated_profile}


def _completeness_output_path(state: DataQualityState) -> str:
    already_run = "completeness_team" in state.get("completed_teams", [])
    if already_run:
        return _versioned_path(state["original_dataset_path"], 2, f"iter{state.get('iteration_count', 1)}")
    return _versioned_path(state["original_dataset_path"], 2)


def _consistency_output_path(state: DataQualityState) -> str:
    already_run = "consistency_team" in state.get("completed_teams", [])
    if already_run:
        return _versioned_path(state["original_dataset_path"], 3, f"iter{state.get('iteration_count', 1)}")
    return _versioned_path(state["original_dataset_path"], 3)


def call_schema_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = schema_graph.invoke(
        {"messages": _team_initial_message(path, "Perform schema validation and schema-level fixes.", profile)}
    )
    summary = _collect_team_summary(result)

    out_path = _versioned_path(state["original_dataset_path"], 1)
    fix_result = apply_schema_fixes(path, out_path, profile)

    report_msg = (
        f"[Schema Team] Findings:\n{summary}\n\n"
        f"Fixes applied: {fix_result['fixes_applied']}\n"
        f"Type changes: {fix_result.get('types_changed_count', 0)}\n"
        f"Fixed CSV: {out_path}"
    )

    return Command(
        update={
            "messages": [HumanMessage(content=report_msg, name="schema_team")],
            "working_dataset_path": out_path,
            "completed_teams": _mark_completed(state, "schema_team"),
            "last_completed_team": "schema",
            "types_changed_count": int(fix_result.get("types_changed_count", 0)),
            "rows_removed_last_team": 0,
        },
        goto="top_supervisor",
    )


def call_completeness_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = completeness_graph.invoke(
        {
            "messages": _team_initial_message(
                path,
                "Perform completeness analysis: detect nulls, rates, and sparse columns.",
                profile,
            )
        }
    )
    summary = _collect_team_summary(result)

    out_path = _completeness_output_path(state)
    fix_result = apply_completeness_fixes(path, out_path, profile)

    report_msg = (
        f"[Completeness Team] Findings:\n{summary}\n\n"
        f"Fixes applied: {fix_result['fixes_applied']}\n"
        f"Fixed CSV: {out_path}"
    )

    return Command(
        update={
            "messages": [HumanMessage(content=report_msg, name="completeness_team")],
            "working_dataset_path": out_path,
            "completed_teams": _mark_completed(state, "completeness_team"),
            "last_completed_team": "completeness",
            "rows_removed_last_team": 0,
        },
        goto="top_supervisor",
    )


def call_consistency_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = consistency_graph.invoke(
        {
            "messages": _team_initial_message(
                path,
                "Perform consistency validation: formats, cross-column logic, and duplicates.",
                profile,
            )
        }
    )
    summary = _collect_team_summary(result)

    out_path = _consistency_output_path(state)
    fix_result = apply_consistency_fixes(path, out_path, profile)
    removed = int(fix_result.get("rows_removed", 0))

    report_msg = (
        f"[Consistency Team] Findings:\n{summary}\n\n"
        f"Fixes applied: {fix_result['fixes_applied']}\n"
        f"Rows removed: {removed}\n"
        f"Fixed CSV: {out_path}"
    )

    return Command(
        update={
            "messages": [HumanMessage(content=report_msg, name="consistency_team")],
            "working_dataset_path": out_path,
            "completed_teams": _mark_completed(state, "consistency_team"),
            "last_completed_team": "consistency",
            "rows_removed": int(state.get("rows_removed", 0)) + removed,
            "rows_removed_last_team": removed,
        },
        goto="top_supervisor",
    )


def call_anomaly_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = anomaly_graph.invoke(
        {
            "messages": _team_initial_message(
                path,
                "Perform anomaly detection: numerical outliers and rare categorical values.",
                profile,
            )
        }
    )
    summary = _collect_team_summary(result)

    report_msg = f"[Anomaly Team] Findings:\n{summary}"
    return Command(
        update={
            "messages": [HumanMessage(content=report_msg, name="anomaly_team")],
            "completed_teams": _mark_completed(state, "anomaly_team"),
            "last_completed_team": "anomaly",
            "rows_removed_last_team": 0,
        },
        goto="top_supervisor",
    )


def call_remediation_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    team_names = ("schema_team", "completeness_team", "consistency_team", "anomaly_team")
    findings: dict[str, str] = {}
    for m in state["messages"]:
        if isinstance(m, HumanMessage) and m.name in team_names:
            findings[m.name] = _extract_text(m.content)

    all_findings_text = "\n\n".join(findings.values())
    findings_payload = json.dumps(
        {
            "all_findings_text": all_findings_text,
            "column_count": len(state["dataset_profile"].get("columns", {})),
            "row_count": state["dataset_profile"].get("total_rows", 0),
        },
        ensure_ascii=False,
    )

    result = remediation_graph.invoke(
        {
            "messages": _team_initial_message(
                findings_payload,
                "Generate correction suggestions and reliability scoring from all prior findings.",
            )
        }
    )
    summary = _collect_team_summary(result)

    suggestions_raw = generate_correction_suggestions.invoke({"findings_json": findings_payload})
    suggestions_data = _tool_output_to_dict(suggestions_raw)
    suggestions = suggestions_data.get("correction_suggestions", [])

    score_raw = calculate_reliability_score.invoke({"findings_json": findings_payload})
    score_data = _tool_output_to_dict(score_raw)

    final_path = _versioned_path(state["original_dataset_path"], 4)
    remediation_result = apply_remediation_fixes(state["working_dataset_path"], final_path, suggestions)

    remediation_summary = (
        f"{summary}\n\n"
        f"Auto-applied fixes: {remediation_result.get('fixes_applied_count', 0)}\n"
        f"Manual-review fixes: {len(remediation_result.get('skipped_fixes', []))}\n"
        f"Expected score after roadmap fixes: {score_data.get('expected_score_after_fixes', 'n/a')}/100"
    )
    findings["remediation_team"] = remediation_summary

    report = {
        "status": "completed",
        "original_path": state["original_dataset_path"],
        "total_messages": len(state["messages"]),
        "teams_executed": sorted(set(state.get("completed_teams", []) + ["remediation_team"]), key=TEAMS.index),
        "teams_skipped": state.get("skipped_teams", []),
        "iteration_count": state.get("iteration_count", 0),
        "rows_removed_total": state.get("rows_removed", 0),
        "supervisor_decisions": state.get("supervisor_decisions", []),
        "fixes_applied": remediation_result.get("applied_fixes", []),
        "manual_review_items": remediation_result.get("skipped_fixes", []),
        "score": score_data,
        "correction_suggestions": suggestions,
        "findings": findings,
    }
    report["supervisor_narrative"] = _generate_supervisor_narrative(report)

    data_dir = Path(state["original_dataset_path"]).parent
    stem = Path(state["original_dataset_path"]).stem
    json_path = data_dir / f"{stem}_quality_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    md_path = data_dir / f"{stem}_quality_report.md"
    md_path.write_text(_build_markdown(report), encoding="utf-8")

    report_msg = (
        f"[Remediation Team] Report:\n{remediation_summary}\n\n"
        f"Final cleaned dataset: {final_path}\n"
        f"JSON report: {json_path}\n"
        f"Markdown report: {md_path}"
    )

    return Command(
        update={
            "messages": [HumanMessage(content=report_msg, name="remediation_team")],
            "working_dataset_path": final_path,
            "completed_teams": _mark_completed(state, "remediation_team"),
            "last_completed_team": "remediation",
        },
        goto="top_supervisor",
    )


# ---------------------------------------------------------------------------
# Build and compile
# ---------------------------------------------------------------------------

def build_graph():
    builder = StateGraph(DataQualityState)

    builder.add_node("profiler", run_profiler)
    builder.add_node("semantic_enricher", run_semantic_enricher)
    builder.add_node("top_supervisor", smart_supervisor_node)
    builder.add_node("schema_team", call_schema_team)
    builder.add_node("completeness_team", call_completeness_team)
    builder.add_node("consistency_team", call_consistency_team)
    builder.add_node("anomaly_team", call_anomaly_team)
    builder.add_node("remediation_team", call_remediation_team)

    builder.add_edge(START, "profiler")
    builder.add_edge("profiler", "semantic_enricher")
    builder.add_edge("semantic_enricher", "top_supervisor")

    return builder.compile()


graph = build_graph()
