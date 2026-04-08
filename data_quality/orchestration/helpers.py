"""Orchestration helpers for the top-level data quality graph."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from langchain_core.messages import HumanMessage

from data_quality.state import DataQualityState


def versioned_path(original_path: str, version: int, suffix: str = "") -> str:
    """Return a versioned CSV path, e.g. `spesa_v1.csv` or `spesa_v2_iter1.csv`."""
    p = Path(original_path)
    suffix_part = f"_{suffix}" if suffix else ""
    return str(p.parent / f"{p.stem}_v{version}{suffix_part}{p.suffix}")


def team_initial_message(working_path: str, task: str, profile: dict | None = None) -> list[dict]:
    """Build the initial user message passed to a team subgraph."""
    content = f"{task}\n\nDataset path: {working_path}"
    if profile:
        content += f"\n\nDataset profile:\n{json.dumps(profile, ensure_ascii=False)}"
    return [{"role": "user", "content": content}]


def extract_text(content: Any) -> str:
    """Normalize model content (string or content blocks) to plain text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            part.get("text", "")
            for part in content
            if isinstance(part, dict) and part.get("type") == "text"
        )
    return str(content)


def collect_team_summary(result: dict) -> str:
    """Collect worker summaries from a team result payload."""
    worker_msgs = [m for m in result["messages"] if isinstance(m, HumanMessage) and m.name]
    if worker_msgs:
        return "\n\n---\n\n".join(extract_text(m.content) for m in worker_msgs)
    return extract_text(result["messages"][-1].content)


def tool_output_to_dict(tool_output: Any) -> dict:
    """Normalize tool output to a dict, parsing JSON strings when possible."""
    if isinstance(tool_output, str):
        try:
            return json.loads(tool_output)
        except json.JSONDecodeError:
            return {"raw": tool_output}
    if isinstance(tool_output, dict):
        return tool_output
    return {"raw": str(tool_output)}


def mark_completed(state: DataQualityState, team_name: str) -> list[str]:
    """Append team to `completed_teams` if missing."""
    completed = list(state.get("completed_teams", []))
    if team_name not in completed:
        completed.append(team_name)
    return completed


def profile_skips(profile: dict) -> list[str]:
    """Compute teams skipped by profile-level deterministic rules."""
    skips = []
    if float(profile.get("overall_completeness", 1.0)) > 0.95:
        skips.append("completeness_team")
    has_anomaly_targets = bool(profile.get("numeric_columns") or profile.get("categorical_columns"))
    if not has_anomaly_targets:
        skips.append("anomaly_team")
    if int(profile.get("total_columns", 0)) < 3 or int(profile.get("total_rows", 0)) < 10:
        skips.append("consistency_team")
    return skips


def iteration_target(state: DataQualityState) -> str | None:
    """Return an iteration re-route target based on post-fix impact metrics."""
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


def eligible_teams(state: DataQualityState) -> list[str]:
    """Return deterministically eligible next teams in priority order."""
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

    if "anomaly_team" not in completed and (
        profile.get("numeric_columns") or profile.get("categorical_columns")
    ):
        eligible.append("anomaly_team")

    if not eligible and "remediation_team" not in completed:
        eligible.append("remediation_team")

    if not eligible:
        return ["FINISH"]
    return eligible


def recent_findings_digest(state: DataQualityState) -> dict[str, str]:
    """Collect short recent team summaries for supervisor LLM context."""
    team_names = {
        "schema_team",
        "completeness_team",
        "consistency_team",
        "anomaly_team",
        "remediation_team",
    }
    digest: dict[str, str] = {}
    for msg in state.get("messages", []):
        if isinstance(msg, HumanMessage) and msg.name in team_names:
            digest[msg.name] = extract_text(msg.content)[:800]
    return digest


def is_borderline_case(state: DataQualityState, eligible: list[str]) -> bool:
    """Heuristic to decide when LLM routing should be considered."""
    profile = state.get("dataset_profile", {})
    completeness = float(profile.get("overall_completeness", 1.0))
    rows = int(profile.get("total_rows", 0))
    cols = int(profile.get("total_columns", 0))
    if 0.93 <= completeness <= 0.97:
        return True
    if 8 <= rows <= 20 or 2 <= cols <= 5:
        return True
    return len(eligible) > 1
