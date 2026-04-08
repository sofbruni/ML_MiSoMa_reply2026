"""
Remediation Team
  Worker 1 : correction_suggester
  Worker 2 : reliability_scorer

Workers run in fixed sequence (deterministic edges — no LLM supervisor needed).
"""

from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, START, END

from data_quality.config import get_llm
from data_quality.tools.remediation_tools import (
    generate_correction_suggestions,
    calculate_reliability_score,
)

llm = get_llm()


class TeamState(MessagesState):
    pass


# ---------------------------------------------------------------------------
# Worker agents
# ---------------------------------------------------------------------------

suggester_agent = create_react_agent(
    llm,
    tools=[generate_correction_suggestions],
    prompt=(
        "You are a Data Quality Remediation Specialist within a multi-agent data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Synthesise all findings from the four preceding analysis teams (Schema, Completeness, "
        "Consistency, Anomaly) into a clear, prioritised list of actionable correction suggestions.\n\n"

        "WORKFLOW:\n"
        "1. You will receive a JSON payload in your context. It contains: "
        "'all_findings_text' (narrative from all four teams), 'column_count', 'row_count'.\n"
        "2. Call generate_correction_suggestions with the FULL JSON payload string.\n"
        "3. Interpret the tool output and produce a comprehensive remediation report.\n\n"

        "HOW TO PRESENT YOUR CORRECTIONS:\n"
        "Group suggestions by priority:\n\n"
        "PRIORITY 1 — REMOVE (immediate, no data loss):\n"
        "  • Drop columns that are >95% empty\n"
        "  • Drop confirmed exact-duplicate columns\n"
        "  • Remove exact duplicate rows\n\n"
        "PRIORITY 2 — RENAME / RESTRUCTURE:\n"
        "  • Rename columns violating snake_case\n"
        "  • Convert hyphenated names to underscores\n\n"
        "PRIORITY 3 — TYPE COERCION:\n"
        "  • Strip currency symbols and coerce to float\n"
        "  • Convert placeholder strings to NaN\n"
        "  • Normalise date columns to ISO-8601 YYYY-MM-DD\n\n"
        "PRIORITY 4 — IMPUTE / FILL:\n"
        "  • Fill numeric columns with median\n"
        "  • Fill categorical columns with mode or 'Unknown'\n\n"
        "PRIORITY 5 — STANDARDISE:\n"
        "  • Normalise case variants in categorical columns\n"
        "  • Unify mixed date formats\n"
        "  • Map rare categorical values (<0.5%) to 'Other' where appropriate\n\n"
        "PRIORITY 6 — INVESTIGATE:\n"
        "  • Near-duplicate rows requiring manual review\n"
        "  • Anomalous outliers that may be legitimate or errors\n"
        "  • Cross-column logic violations\n"
        "  • Unexpected negative values in numeric columns\n\n"
        "END WITH: A summary table — Priority | Column(s) | Issue | Action | Expected Outcome.\n\n"

        "STRICT RULES:\n"
        "- Base ALL suggestions on the actual findings. Do not invent issues.\n"
        "- Every suggestion must be actionable by a data engineer.\n"
        "- Quantify expected outcomes where possible.\n"
    ),
    name="correction_suggester",
)

scorer_agent = create_react_agent(
    llm,
    tools=[calculate_reliability_score],
    prompt=(
        "You are a Data Reliability Scoring specialist within a multi-agent data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Compute a quantitative reliability score (0–100) for the dataset based on all findings. "
        "The score and its breakdown must be transparent and actionable.\n\n"

        "WORKFLOW:\n"
        "1. You will receive a JSON payload in your context.\n"
        "2. Call calculate_reliability_score with the FULL JSON payload string.\n"
        "3. Interpret the tool output and produce a complete reliability assessment report.\n\n"

        "SCORING METHODOLOGY (starts at 100, deductions applied):\n"
        "  Schema: -3 pts per type issue (scaled), -1 pt per naming issue (scaled); max -20\n"
        "  Completeness: -(100 - completeness_pct) × 0.5 pts (global)\n"
        "  Consistency: -3 pts per format issue (scaled), -5 for cross-column violations,\n"
        "               -min(duplicates/100, 10) pts for duplicate rows\n"
        "  Anomaly: -min(outlier_count/50, 10) pts per column with IQR outliers\n"
        "  Wide-dataset scaling: col_scale = min(10/column_count, 1.0) applied to per-column deductions\n\n"

        "HOW TO PRESENT THE SCORE:\n"
        "Section 1 — Final Score and Grade:\n"
        "  Display: 'Reliability Score: XX/100 — Grade: Y'\n"
        "  Grade thresholds: A (≥90), B (≥75), C (≥60), D (≥45), F (<45)\n"
        "  1–2 sentences interpreting the grade and what it means for using this dataset.\n\n"
        "Section 2 — Deduction Breakdown:\n"
        "  Table: Category | Issue | Deduction | Running Score (starting from 100).\n\n"
        "Section 3 — Score Improvement Roadmap:\n"
        "  Top 3 changes with highest score impact and estimated points recovered.\n\n"
        "Section 4 — Expected Post-Fix Score:\n"
        "  Estimate the score after all Priority 1–5 corrections from the correction_suggester.\n\n"

        "STRICT RULES:\n"
        "- Use the tool output for the numerical score — do not compute it manually.\n"
        "- Grade thresholds are fixed: A≥90, B≥75, C≥60, D≥45, F<45.\n"
        "- End your report with: 'Score: XX/100  Grade: Y' (for automatic extraction).\n"
    ),
    name="reliability_scorer",
)

# ---------------------------------------------------------------------------
# Worker node wrappers (plain dict return — edges handle routing)
# ---------------------------------------------------------------------------

def _task_only(state: TeamState) -> dict:
    """Return only the first (task) message so agents don't get confused by prior outputs."""
    return {"messages": state["messages"][:1]}


def suggester_node(state: TeamState) -> dict:
    result = suggester_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="correction_suggester"
    )]}


def scorer_node(state: TeamState) -> dict:
    result = scorer_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="reliability_scorer"
    )]}


# ---------------------------------------------------------------------------
# Compiled subgraph — deterministic fixed edges, no LLM supervisor
# ---------------------------------------------------------------------------

remediation_graph = (
    StateGraph(TeamState)
    .add_node("correction_suggester", suggester_node)
    .add_node("reliability_scorer", scorer_node)
    .add_edge(START, "correction_suggester")
    .add_edge("correction_suggester", "reliability_scorer")
    .add_edge("reliability_scorer", END)
    .compile()
)
