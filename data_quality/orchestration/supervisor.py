"""Hybrid top supervisor logic (deterministic guardrails + optional LLM choice)."""

from __future__ import annotations

import json
from typing import Any, Literal

from typing_extensions import TypedDict
from langgraph.graph import END
from langgraph.types import Command

from data_quality.config import get_llm
from data_quality.state import DataQualityState
from data_quality.orchestration.constants import TEAMS, LLM_SUPERVISOR_MIN_CONFIDENCE
from data_quality.orchestration.helpers import (
    eligible_teams,
    is_borderline_case,
    iteration_target,
    profile_skips,
    recent_findings_digest,
)

llm = get_llm()


class LLMRoute(TypedDict):
    next: Literal[
        "schema_team",
        "completeness_team",
        "consistency_team",
        "anomaly_team",
        "remediation_team",
        "FINISH",
    ]
    reason: str
    confidence: float
    expected_impact: str


def _llm_supervisor_choice(state: DataQualityState, eligible: list[str]) -> dict[str, Any] | None:
    """Use LLM to choose next team within deterministic eligibility constraints."""
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
        "recent_findings_digest": recent_findings_digest(state),
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
    """Append one supervisor decision event into state telemetry."""
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


def smart_supervisor_node(
    state: DataQualityState,
) -> Command[Literal[*TEAMS, "__end__"]]:  # type: ignore[valid-type]
    """Route to the next team using hybrid policy.

    Deterministic rules always apply first. LLM routing is only used for
    ambiguous/borderline cases and never outside eligible teams.
    """
    profile = state.get("dataset_profile", {})
    eligible = eligible_teams(state)

    pending_iteration = iteration_target(state)
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
                "skipped_teams": profile_skips(profile),
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
        return Command(
            goto=END,
            update={
                "next": "FINISH",
                "skipped_teams": profile_skips(profile),
                "supervisor_decisions": decisions,
            },
        )

    selected = eligible[0]
    source = "deterministic"
    reason = "Top eligible team by deterministic policy."
    confidence = 1.0

    if is_borderline_case(state, eligible):
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
            "skipped_teams": profile_skips(profile),
            "supervisor_decisions": decisions,
        },
    )
