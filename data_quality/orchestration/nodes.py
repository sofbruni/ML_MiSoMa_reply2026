"""Top-level graph nodes (profiler, team calls, remediation finalization)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from langchain_core.messages import HumanMessage
from langgraph.types import Command

from data_quality.config import get_llm
from data_quality.orchestration.constants import TEAMS
from data_quality.orchestration.helpers import (
    collect_team_summary,
    mark_completed,
    team_initial_message,
    tool_output_to_dict,
    versioned_path,
    extract_text,
)
from data_quality.orchestration.reporting import build_markdown, generate_supervisor_narrative
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

llm = get_llm()


def run_profiler(state: DataQualityState) -> dict:
    """Create the dataset profile and initialize orchestration metadata."""
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
    """Run one LLM enrichment pass and attach enrichments to the profile."""
    profile = state["dataset_profile"]
    enrichments = enrich_profile(profile, llm)
    return {"dataset_profile": {**profile, "enrichments": enrichments}}


def _completeness_output_path(state: DataQualityState) -> str:
    already_run = "completeness_team" in state.get("completed_teams", [])
    if already_run:
        return versioned_path(
            state["original_dataset_path"],
            2,
            f"iter{state.get('iteration_count', 1)}",
        )
    return versioned_path(state["original_dataset_path"], 2)


def _consistency_output_path(state: DataQualityState) -> str:
    already_run = "consistency_team" in state.get("completed_teams", [])
    if already_run:
        return versioned_path(
            state["original_dataset_path"],
            3,
            f"iter{state.get('iteration_count', 1)}",
        )
    return versioned_path(state["original_dataset_path"], 3)


def call_schema_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    """Run schema team subgraph and apply deterministic schema fixes."""
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = schema_graph.invoke({
        "messages": team_initial_message(
            path,
            "Perform schema validation and schema-level fixes.",
            profile,
        )
    })
    summary = collect_team_summary(result)

    out_path = versioned_path(state["original_dataset_path"], 1)
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
            "completed_teams": mark_completed(state, "schema_team"),
            "last_completed_team": "schema",
            "types_changed_count": int(fix_result.get("types_changed_count", 0)),
            "rows_removed_last_team": 0,
        },
        goto="top_supervisor",
    )


def call_completeness_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    """Run completeness team subgraph and apply completeness fixes."""
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = completeness_graph.invoke({
        "messages": team_initial_message(
            path,
            "Perform completeness analysis: detect nulls, rates, and sparse columns.",
            profile,
        )
    })
    summary = collect_team_summary(result)

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
            "completed_teams": mark_completed(state, "completeness_team"),
            "last_completed_team": "completeness",
            "rows_removed_last_team": 0,
        },
        goto="top_supervisor",
    )


def call_consistency_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    """Run consistency team subgraph and apply consistency fixes."""
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = consistency_graph.invoke({
        "messages": team_initial_message(
            path,
            "Perform consistency validation: formats, cross-column logic, and duplicates.",
            profile,
        )
    })
    summary = collect_team_summary(result)

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
            "completed_teams": mark_completed(state, "consistency_team"),
            "last_completed_team": "consistency",
            "rows_removed": int(state.get("rows_removed", 0)) + removed,
            "rows_removed_last_team": removed,
        },
        goto="top_supervisor",
    )


def call_anomaly_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    """Run anomaly team subgraph (findings only, no direct CSV mutation)."""
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = anomaly_graph.invoke({
        "messages": team_initial_message(
            path,
            "Perform anomaly detection: numerical outliers and rare categorical values.",
            profile,
        )
    })
    summary = collect_team_summary(result)

    report_msg = f"[Anomaly Team] Findings:\n{summary}"
    return Command(
        update={
            "messages": [HumanMessage(content=report_msg, name="anomaly_team")],
            "completed_teams": mark_completed(state, "anomaly_team"),
            "last_completed_team": "anomaly",
            "rows_removed_last_team": 0,
        },
        goto="top_supervisor",
    )


def call_remediation_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    """Aggregate findings, run remediation/scoring, persist final artifacts."""
    team_names = ("schema_team", "completeness_team", "consistency_team", "anomaly_team")
    findings: dict[str, str] = {}
    for message in state["messages"]:
        if isinstance(message, HumanMessage) and message.name in team_names:
            findings[message.name] = extract_text(message.content)

    all_findings_text = "\n\n".join(findings.values())
    findings_payload = json.dumps(
        {
            "all_findings_text": all_findings_text,
            "column_count": len(state["dataset_profile"].get("columns", {})),
            "row_count": state["dataset_profile"].get("total_rows", 0),
        },
        ensure_ascii=False,
    )

    result = remediation_graph.invoke({
        "messages": team_initial_message(
            findings_payload,
            "Generate correction suggestions and reliability scoring from all prior findings.",
        )
    })
    summary = collect_team_summary(result)

    suggestions_raw = generate_correction_suggestions.invoke({"findings_json": findings_payload})
    suggestions_data = tool_output_to_dict(suggestions_raw)
    suggestions = suggestions_data.get("correction_suggestions", [])

    score_raw = calculate_reliability_score.invoke({"findings_json": findings_payload})
    score_data = tool_output_to_dict(score_raw)

    final_path = versioned_path(state["original_dataset_path"], 4)
    remediation_result = apply_remediation_fixes(
        state["working_dataset_path"],
        final_path,
        suggestions,
    )

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
        "teams_executed": sorted(
            set(state.get("completed_teams", []) + ["remediation_team"]),
            key=TEAMS.index,
        ),
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
    report["supervisor_narrative"] = generate_supervisor_narrative(report)

    data_dir = Path(state["original_dataset_path"]).parent
    stem = Path(state["original_dataset_path"]).stem

    json_path = data_dir / f"{stem}_quality_report.json"
    with open(json_path, "w", encoding="utf-8") as file_obj:
        json.dump(report, file_obj, indent=2, ensure_ascii=False)

    md_path = data_dir / f"{stem}_quality_report.md"
    md_path.write_text(build_markdown(report), encoding="utf-8")

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
            "completed_teams": mark_completed(state, "remediation_team"),
            "last_completed_team": "remediation",
        },
        goto="top_supervisor",
    )
