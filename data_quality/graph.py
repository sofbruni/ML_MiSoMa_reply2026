"""
Top-level hierarchical graph
────────────────────────────
Profiler (dataset profile detection — pure pandas, no LLM)
  ↓
Top Supervisor
 ├── call_schema_team       → schema_graph     → apply_schema_fixes(profile)
 ├── call_completeness_team → completeness_graph → apply_completeness_fixes(profile)
 ├── call_consistency_team  → consistency_graph  → apply_consistency_fixes(profile)
 ├── call_anomaly_team      → anomaly_graph      (no CSV changes — detection only)
 └── call_remediation_team  → remediation_graph  → build_final_report()

Each team node:
  1. Invokes the compiled team subgraph with the current working_dataset_path
     AND the dataset profile embedded in the initial message
  2. Extracts the last AI message (findings summary) from the subgraph response
  3. Calls the corresponding fix function (passing the profile) to produce the next CSV
  4. Updates working_dataset_path in the parent state
  5. Returns to the top supervisor
"""

import json
import os
import re
import shutil
from datetime import date
from pathlib import Path
from typing import Literal
from typing_extensions import TypedDict

from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, START, END
from langgraph.types import Command

from data_quality.config import get_llm
from data_quality.state import DataQualityState
from data_quality.teams.schema_team import schema_graph
from data_quality.teams.completeness_team import completeness_graph
from data_quality.teams.consistency_team import consistency_graph
from data_quality.teams.anomaly_team import anomaly_graph
from data_quality.teams.remediation_team import remediation_graph
from data_quality.tools.profiler import profile_dataset
from data_quality.tools.semantic_enricher import enrich_profile
from data_quality.tools.schema_tools import apply_schema_fixes
from data_quality.tools.completeness_tools import apply_completeness_fixes
from data_quality.tools.consistency_tools import apply_consistency_fixes
from data_quality.tools.remediation_tools import (
    build_final_report,
    calculate_reliability_score,
    generate_correction_suggestions,
)

llm = get_llm()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TEAMS = ["schema_team", "completeness_team", "consistency_team", "anomaly_team", "remediation_team"]


def _versioned_path(original_path: str, version: int) -> str:
    """Return e.g.  data/spesa_v1.csv from data/spesa.csv and version=1."""
    p = Path(original_path)
    return str(p.parent / f"{p.stem}_v{version}{p.suffix}")


def _team_initial_message(working_path: str, task: str, profile: dict | None = None) -> list[dict]:
    """Build the initial user message for a team subgraph.

    The dataset profile is embedded as JSON so worker agents know what columns
    exist and what their semantic types are, without hardcoding anything.
    """
    content = f"{task}\n\nDataset path: {working_path}"
    if profile:
        profile_json = json.dumps(profile, ensure_ascii=False)
        content += f"\n\nDataset profile (column names and semantic types):\n{profile_json}"
    return [{"role": "user", "content": content}]


def _extract_text(content) -> str:
    """Unwrap Gemini content-block lists into a plain string.
    Gemini may return content as [{'type': 'text', 'text': '...'}] instead of str."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            part["text"] for part in content
            if isinstance(part, dict) and part.get("type") == "text"
        )
    return str(content)


def _collect_team_summary(result: dict) -> str:
    """Collect and concatenate all worker HumanMessages from a team result."""
    worker_msgs = [m for m in result["messages"] if isinstance(m, HumanMessage) and m.name]
    if worker_msgs:
        return "\n\n---\n\n".join(_extract_text(m.content) for m in worker_msgs)
    return _extract_text(result["messages"][-1].content)


# ---------------------------------------------------------------------------
# Markdown report builder (self-contained, no external dependencies)
# ---------------------------------------------------------------------------

_TEAM_META = {
    "schema_team":       {"title": "Schema Validation",       "emoji": "🗂️",  "desc": "Data types, column naming conventions, and duplicate columns."},
    "completeness_team": {"title": "Completeness Analysis",   "emoji": "🔍",  "desc": "Missing values, null rates, and sparse column detection."},
    "consistency_team":  {"title": "Consistency Validation",  "emoji": "✅",  "desc": "Format normalisation, cross-column logic, and duplicate rows."},
    "anomaly_team":      {"title": "Anomaly Detection",       "emoji": "⚠️",  "desc": "Numerical outliers (IQR / Z-score) and rare categorical values."},
    "remediation_team":  {"title": "Remediation & Reliability","emoji": "🛠️", "desc": "Correction suggestions and final reliability score."},
}
_TEAM_ORDER = list(_TEAM_META.keys())

_SKIP_PREFIXES = (
    "[schema team]", "[completeness team]", "[consistency team]",
    "[anomaly team]", "[remediation team]",
    "fixes applied:", "fixed csv:", "final cleaned dataset:",
)


def _clean_findings(raw: str) -> str:
    lines = [l for l in raw.splitlines()
             if not any(l.lower().strip().startswith(p) for p in _SKIP_PREFIXES)]
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
    total_msg = report.get("total_messages", 0)
    score, grade = _extract_score(findings.get("remediation_team", ""))

    lines: list[str] = []

    # Title table
    lines += [
        "# Data Quality Report", "",
        "| | |", "|---|---|",
        f"| **Dataset** | `{dataset_name}` |",
        f"| **Date** | {today} |",
        f"| **Teams executed** | {len(teams_executed)} |",
        f"| **Messages exchanged** | {total_msg} |",
    ]
    if score:
        grade_part = f"  —  Grade **{grade}**" if grade else ""
        lines.append(f"| **Reliability score** | **{score}/100**{grade_part} |")
    lines += ["", "---", ""]

    # Table of contents
    lines += ["## Contents", ""]
    lines.append("1. [Executive Summary](#executive-summary)")
    for i, key in enumerate(_TEAM_ORDER, start=2):
        if key in findings:
            meta = _TEAM_META[key]
            anchor = meta["title"].lower().replace(" ", "-").replace("&", "").replace("--", "-")
            lines.append(f"{i}. [{meta['emoji']} {meta['title']}](#{anchor})")
    lines += ["", "---", ""]

    # Executive summary
    lines += ["## Executive Summary", ""]
    if score:
        label = f"{score}/100" + (f"  |  Grade {grade}" if grade else "")
        lines.append(f"> **Reliability Score: {label}**\n")
    lines += [
        "The pipeline ran **5 specialist teams** in sequence. Each team analysed the",
        "working CSV and applied targeted fixes before passing control to the next team.",
        "The table below summarises what each team was responsible for.",
        "",
        "| Team | Responsibility | Status |",
        "|------|---------------|--------|",
    ]
    for key in _TEAM_ORDER:
        meta = _TEAM_META[key]
        status = "✅ Completed" if key in teams_executed else "⏭️ Skipped"
        lines.append(f"| {meta['emoji']} **{meta['title']}** | {meta['desc']} | {status} |")
    lines += ["", "---", ""]

    # One section per team
    for key in _TEAM_ORDER:
        if key not in findings:
            continue
        meta = _TEAM_META[key]
        lines += [
            f"## {meta['emoji']} {meta['title']}", "",
            f"> *{meta['desc']}*", "",
            _clean_findings(findings[key]),
            "", "---", "",
        ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Profiler node
# ---------------------------------------------------------------------------

def run_profiler(state: DataQualityState) -> dict:
    """Profiles the dataset once before the supervisor starts.
    Detects column semantic types via pandas heuristics (no LLM).
    Stores the profile in state — does NOT add to message history so the
    supervisor sees only the original task request and routes correctly.
    """
    prof = profile_dataset(state["original_dataset_path"])
    col_summary = ", ".join(
        f"{c}: {v['semantic_type']}" for c, v in prof["columns"].items()
    )
    print(f"  Columns: {col_summary}")
    return {"dataset_profile": prof}


def run_semantic_enricher(state: DataQualityState) -> dict:
    """Single LLM call that reads column names + sample values and assigns
    intended semantic types and fix actions. Enriches dataset_profile in-place.
    Never crashes the pipeline — returns empty enrichments on failure.
    """
    prof = state["dataset_profile"]
    enrichments = enrich_profile(prof, llm)
    if enrichments:
        print(f"  Semantic enrichments: {list(enrichments.keys())}")
        for col, enr in enrichments.items():
            print(f"    {col}: {enr['fix_action']}({enr['params']}) — {enr['notes']}")
    else:
        print("  Semantic enrichments: none")
    updated_profile = {**prof, "enrichments": enrichments}
    return {"dataset_profile": updated_profile}


# ---------------------------------------------------------------------------
# Top supervisor
# ---------------------------------------------------------------------------

def make_top_supervisor():
    options = ["FINISH"] + TEAMS
    system_prompt = (
        "You are the top-level Data Quality supervisor managing 5 specialist teams:\n"
        "  1. schema_team         — validates and fixes data types and column naming\n"
        "  2. completeness_team   — detects and fills missing/null values\n"
        "  3. consistency_team    — normalises formats and removes duplicates\n"
        "  4. anomaly_team        — detects statistical outliers and rare categories\n"
        "  5. remediation_team    — produces correction suggestions and the reliability score\n\n"
        "Execute the teams strictly in order 1→2→3→4→5. "
        "After all five teams have reported, respond with FINISH."
    )

    class Router(TypedDict):
        next: Literal[*options]  # type: ignore[valid-type]

    def top_supervisor_node(
        state: DataQualityState,
    ) -> Command[Literal[*TEAMS, "__end__"]]:  # type: ignore[valid-type]
        messages = [{"role": "system", "content": system_prompt}] + state["messages"]
        response = llm.with_structured_output(Router).invoke(messages)
        goto = response["next"]
        if goto == "FINISH":
            goto = END
        return Command(goto=goto, update={"next": goto})

    return top_supervisor_node


top_supervisor = make_top_supervisor()

# ---------------------------------------------------------------------------
# Team node wrappers
# ---------------------------------------------------------------------------

def call_schema_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = schema_graph.invoke({
        "messages": _team_initial_message(
            path,
            "Perform schema validation: check data types and naming conventions.",
            profile,
        )
    })
    summary = _collect_team_summary(result)
    out_path = _versioned_path(state["original_dataset_path"], 1)
    fix_result = apply_schema_fixes(path, out_path, profile)
    report_msg = (
        f"[Schema Team] Findings:\n{summary}\n\n"
        f"Fixes applied: {fix_result['fixes_applied']}\n"
        f"Fixed CSV: {out_path}"
    )
    return Command(
        update={
            "messages": [HumanMessage(content=report_msg, name="schema_team")],
            "working_dataset_path": out_path,
        },
        goto="top_supervisor",
    )


def call_completeness_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = completeness_graph.invoke({
        "messages": _team_initial_message(
            path,
            "Perform completeness analysis: detect nulls, calculate rates, find sparse columns.",
            profile,
        )
    })
    summary = _collect_team_summary(result)
    out_path = _versioned_path(state["original_dataset_path"], 2)
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
        },
        goto="top_supervisor",
    )


def call_consistency_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = consistency_graph.invoke({
        "messages": _team_initial_message(
            path,
            "Perform consistency validation: check formats, cross-column logic, and duplicates.",
            profile,
        )
    })
    summary = _collect_team_summary(result)
    out_path = _versioned_path(state["original_dataset_path"], 3)
    fix_result = apply_consistency_fixes(path, out_path, profile)
    report_msg = (
        f"[Consistency Team] Findings:\n{summary}\n\n"
        f"Fixes applied: {fix_result['fixes_applied']}\n"
        f"Fixed CSV: {out_path}"
    )
    return Command(
        update={
            "messages": [HumanMessage(content=report_msg, name="consistency_team")],
            "working_dataset_path": out_path,
        },
        goto="top_supervisor",
    )


def call_anomaly_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:
    path = state["working_dataset_path"]
    profile = state["dataset_profile"]
    result = anomaly_graph.invoke({
        "messages": _team_initial_message(
            path,
            "Perform anomaly detection: identify numerical outliers and rare categorical values.",
            profile,
        )
    })
    summary = _collect_team_summary(result)

    # Anomaly team: detection only, no CSV modification
    report_msg = f"[Anomaly Team] Findings:\n{summary}"
    return Command(
        update={
            "messages": [HumanMessage(content=report_msg, name="anomaly_team")],
            # working_dataset_path unchanged
        },
        goto="top_supervisor",
    )


def call_remediation_team(state: DataQualityState) -> Command[Literal["top_supervisor"]]:


    # Collect all team findings from message history
    team_names = ("schema_team", "completeness_team", "consistency_team", "anomaly_team")
    findings: dict[str, str] = {}
    for m in state["messages"]:
        if isinstance(m, HumanMessage) and m.name in team_names:
            findings[m.name] = _extract_text(m.content)

    all_findings_text = "\n\n".join(findings.values())
    # Include dataset-level metadata so scoring can scale per-column deductions
    findings_payload = json.dumps({
        "all_findings_text": all_findings_text,
        "column_count": len(state["dataset_profile"].get("columns", {})),
        "row_count": state["dataset_profile"].get("row_count", 0),
    })

    result = remediation_graph.invoke({
        "messages": _team_initial_message(
            findings_payload,
            "Generate correction suggestions and compute the reliability score "
            "based on all findings above.",
        )
    })
    summary = _collect_team_summary(result)
    findings["remediation_team"] = summary

    # Save final cleaned CSV
    final_path = _versioned_path(state["original_dataset_path"], 4)
    shutil.copy2(state["working_dataset_path"], final_path)

    # Build and save JSON report
    report = {
        "status": "completed",
        "original_path": state["original_dataset_path"],
        "total_messages": len(state["messages"]),
        "teams_executed": [t for t in (*team_names, "remediation_team") if t in findings],
        "findings": findings,
    }
    data_dir = Path(state["original_dataset_path"]).parent
    stem = Path(state["original_dataset_path"]).stem  # e.g. "spesa" or "attivazioniCessazioni"
    json_path = data_dir / f"{stem}_quality_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # Build and save Markdown report
    md_path = data_dir / f"{stem}_quality_report.md"
    md_path.write_text(_build_markdown(report), encoding="utf-8")

    report_msg = (
        f"[Remediation Team] Report:\n{summary}\n\n"
        f"Final cleaned dataset: {final_path}\n"
        f"JSON report: {json_path}\n"
        f"Markdown report: {md_path}"
    )
    return Command(
        update={
            "messages": [HumanMessage(content=report_msg, name="remediation_team")],
            "working_dataset_path": final_path,
        },
        goto="top_supervisor",
    )


# ---------------------------------------------------------------------------
# Build and compile the top-level graph
# ---------------------------------------------------------------------------

def build_graph():
    builder = StateGraph(DataQualityState)

    builder.add_node("profiler", run_profiler)
    builder.add_node("semantic_enricher", run_semantic_enricher)
    builder.add_node("top_supervisor", top_supervisor)
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
