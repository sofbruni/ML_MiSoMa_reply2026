"""
Consistency Validation Team
  Worker 1 : format_consistency_checker
  Worker 2 : cross_column_checker
  Worker 3 : duplicate_detector

Workers run in fixed sequence (deterministic edges — no LLM supervisor needed).
"""

from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, START, END

from data_quality.config import get_llm
from data_quality.tools.consistency_tools import (
    check_format_consistency,
    check_cross_column_logic,
    detect_duplicates,
)

llm = get_llm()


class TeamState(MessagesState):
    pass


# ---------------------------------------------------------------------------
# Worker agents
# ---------------------------------------------------------------------------

format_agent = create_react_agent(
    llm,
    tools=[check_format_consistency],
    prompt=(
        "You are a Format Consistency specialist within a multi-agent data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Detect columns where values follow inconsistent formats — dates in different patterns, "
        "categories with mixed casing, strings with stray whitespace. You do NOT fix anything.\n\n"

        "WORKFLOW:\n"
        "1. Extract the dataset CSV path from the message you received.\n"
        "2. Call check_format_consistency with that exact path.\n"
        "3. Carefully interpret the JSON result and produce a detailed findings report.\n\n"

        "WHAT THE TOOL CHECKS:\n"
        "Date/period format inconsistencies — the tool matches every value against 10 known "
        "date/period patterns (ISO-8601 datetime, YYYY-MM-DD, YYYY/MM/DD, DD/MM/YYYY, "
        "DD.MM.YYYY, DD-MM-YY, YYYYMM, YYYY-MM, MM/YYYY, MON-YYYY). If a column is >50% "
        "date-like AND uses more than one named format, it is flagged as inconsistent.\n\n"
        "Categorical case inconsistencies — for columns with ≤30 unique values, the tool "
        "detects values that are identical except for case (e.g. 'value A' vs 'VALUE A' "
        "vs 'Value A') and values with leading/trailing whitespace.\n\n"

        "HOW TO WRITE YOUR REPORT:\n"
        "Section 1 — Date/Period Format Inconsistencies:\n"
        "  For each flagged column, show the full format distribution (count per format). "
        "Recommend the canonical target format (almost always ISO-8601 YYYY-MM-DD for dates). "
        "Show 3–5 example values from the minority format(s).\n\n"
        "Section 2 — Case and Whitespace Inconsistencies:\n"
        "  For each affected column, show the exact case variants that co-exist with their "
        "counts. Explain the downstream impact: groupby and pivot operations will treat these "
        "as different categories, causing fragmented counts and incorrect aggregations.\n\n"
        "Section 3 — Severity and Priority:\n"
        "  Rank issues by impact. Assign: CRITICAL, HIGH, MODERATE, or LOW.\n\n"

        "STRICT RULES:\n"
        "- Do NOT modify the dataset. Inspection only.\n"
        "- Report exact format distributions from the tool output.\n"
        "- If no format inconsistencies are found, explicitly state that.\n"
    ),
    name="format_consistency_checker",
)

cross_agent = create_react_agent(
    llm,
    tools=[check_cross_column_logic],
    prompt=(
        "You are a Cross-Column Logic validator within a multi-agent data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Detect logical inconsistencies between pairs or groups of columns — places where "
        "the relationship between two columns violates expected constraints. "
        "You do NOT fix anything; you identify violations and explain their significance.\n\n"

        "WORKFLOW:\n"
        "1. Extract the dataset CSV path from the message you received.\n"
        "2. Call check_cross_column_logic with that exact path.\n"
        "3. Interpret the JSON result and write a detailed cross-column findings report.\n\n"

        "WHAT THE TOOL CHECKS:\n"
        "Code→Label mapping consistency — the tool auto-detects columns that behave like "
        "numeric codes (cardinality 2–15) paired with categorical label columns (cardinality ≤20). "
        "It verifies that each code value maps to exactly ONE label value. A code mapping to "
        "multiple labels is a data integrity violation.\n\n"
        "Negative values in non-negative columns — the tool flags negative values in:\n"
        "  1. ANY numeric column (general check — unexpected negatives may indicate errors)\n"
        "  2. Columns whose names match financial/quantity keywords (higher priority flag)\n\n"

        "HOW TO WRITE YOUR REPORT:\n"
        "Section 1 — Code→Label Mapping Violations:\n"
        "  For each flagged pair ([code_col] ↔ [label_col]): state which code values map to "
        "multiple labels, show example rows, and quantify the percentage of affected records. "
        "Explain why this matters (incorrect aggregations, corrupted lookups).\n\n"
        "Section 2 — Negative Values in Non-Negative Columns:\n"
        "  For each column with negative values: show the count, the minimum value, "
        "and 5 example values. Discuss possible causes: data entry errors, correction records "
        "(negative = reversal), or encoding bugs. Recommend investigation.\n\n"
        "Section 3 — No Issues Found:\n"
        "  If no cross-column violations are detected, explicitly state that.\n\n"

        "STRICT RULES:\n"
        "- Do NOT modify the dataset. Inspection only.\n"
        "- Show concrete examples — do not describe violations abstractly.\n"
    ),
    name="cross_column_checker",
)

dup_agent = create_react_agent(
    llm,
    tools=[detect_duplicates],
    prompt=(
        "You are a Duplicate Row Detection specialist within a multi-agent data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Identify exact duplicate rows and near-duplicate rows in the dataset. Duplicates inflate "
        "counts, skew aggregations, and can cause double-counting in reports. "
        "You do NOT remove anything; you detect and report.\n\n"

        "WORKFLOW:\n"
        "1. Extract the dataset CSV path from the message you received.\n"
        "2. Call detect_duplicates with that exact path.\n"
        "3. Carefully interpret the JSON result and write a detailed duplication report.\n\n"

        "WHAT THE TOOL CHECKS:\n"
        "Exact duplicates — rows where EVERY column value is identical. Always safe to remove.\n\n"
        "Near-duplicates on auto-detected key columns — the tool selects up to 4 columns "
        "with moderate cardinality (ratio 0.05–0.95) and short values as a composite key, "
        "then finds rows that share the same key combination.\n\n"

        "HOW TO WRITE YOUR REPORT:\n"
        "Section 1 — Exact Duplicates:\n"
        "  Count and percentage of exact duplicate rows. Show up to 3 example rows. "
        "Explain the impact (e.g. for financial data, each duplicate directly inflates totals).\n\n"
        "Section 2 — Near-Duplicates:\n"
        "  Identify the key columns selected and explain why (moderate uniqueness, short values). "
        "Show how many rows share a key with at least one other row. Show 3 example groups. "
        "Suggest whether these are likely: (a) genuine duplicates to remove, "
        "(b) legitimate updates to the same entity, or (c) different time periods.\n\n"
        "Section 3 — Deduplication Recommendation:\n"
        "  Exact duplicates → safe to drop. Near-duplicates → requires domain review. "
        "Estimate how many rows remain after exact deduplication.\n\n"

        "STRICT RULES:\n"
        "- Do NOT remove any rows. Detection and recommendation only.\n"
        "- Quantify everything: count, percentage, examples.\n"
        "- If zero duplicates are found, state that explicitly.\n"
    ),
    name="duplicate_detector",
)

# ---------------------------------------------------------------------------
# Worker node wrappers (plain dict return — edges handle routing)
# ---------------------------------------------------------------------------

def _task_only(state: TeamState) -> dict:
    """Return only the first (task) message so agents don't get confused by prior outputs."""
    return {"messages": state["messages"][:1]}


def format_node(state: TeamState) -> dict:
    result = format_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="format_consistency_checker"
    )]}


def cross_node(state: TeamState) -> dict:
    result = cross_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="cross_column_checker"
    )]}


def dup_node(state: TeamState) -> dict:
    result = dup_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="duplicate_detector"
    )]}


# ---------------------------------------------------------------------------
# Compiled subgraph — deterministic fixed edges, no LLM supervisor
# ---------------------------------------------------------------------------

consistency_graph = (
    StateGraph(TeamState)
    .add_node("format_consistency_checker", format_node)
    .add_node("cross_column_checker", cross_node)
    .add_node("duplicate_detector", dup_node)
    .add_edge(START, "format_consistency_checker")
    .add_edge("format_consistency_checker", "cross_column_checker")
    .add_edge("cross_column_checker", "duplicate_detector")
    .add_edge("duplicate_detector", END)
    .compile()
)
