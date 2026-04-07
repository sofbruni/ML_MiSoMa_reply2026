"""
Completeness Analysis Team
  Worker 1 : null_detector
  Worker 2 : completeness_rate_calculator
  Worker 3 : sparse_column_detector

Workers run in fixed sequence (deterministic edges — no LLM supervisor needed).
"""

from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, START, END

from data_quality.config import get_llm
from data_quality.tools.completeness_tools import (
    detect_missing_values,
    calculate_completeness_rate,
    detect_sparse_columns,
)

llm = get_llm()


class TeamState(MessagesState):
    pass


# ---------------------------------------------------------------------------
# Worker agents
# ---------------------------------------------------------------------------

null_agent = create_react_agent(
    llm,
    tools=[detect_missing_values],
    prompt=(
        "You are a Missing Value and Placeholder Detection specialist within a multi-agent "
        "data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Detect every form of 'missing' data in the dataset — not just NaN/null, but also "
        "placeholder strings that masquerade as real values. You do NOT fix anything.\n\n"

        "WORKFLOW:\n"
        "1. Extract the dataset CSV path from the message you received.\n"
        "2. Call detect_missing_values with that exact path.\n"
        "3. Carefully interpret the JSON result and write a detailed findings report.\n\n"

        "WHAT COUNTS AS MISSING:\n"
        "True nulls/NaN are obvious, but datasets often also contain placeholder strings:\n"
        "  • Abbreviations for 'not available': N.D., ND, N/A, n.d., na\n"
        "  • Ad-hoc null markers: //, ?, -, empty string\n"
        "  • Textual placeholders: 'Unknown', 'TBD', 'PENDING', 'to be defined'\n"
        "The tool detects and counts all of these, including true NaN.\n\n"

        "HOW TO WRITE YOUR REPORT:\n"
        "For each column with ANY missing values:\n"
        "  - Column name and its semantic type (from the profile in your context)\n"
        "  - Count and percentage of truly null values\n"
        "  - Count and percentage of placeholder string occurrences (if any)\n"
        "  - Combined effective missing rate\n"
        "  - Severity: CRITICAL (>95%), HIGH (20–95%), MODERATE (5–20%), LOW (<5%)\n\n"
        "End with a summary table:\n"
        "  Column | Null Count | Null % | Placeholder Count | Effective Missing % | Severity\n\n"
        "Also state: total rows in the dataset, and how many rows have at least one missing value.\n\n"

        "STRICT RULES:\n"
        "- Do NOT modify the dataset. Inspection only.\n"
        "- Report every column with missing values, no matter how small the count.\n"
        "- If a column is 100% complete, do NOT include it in the report.\n"
    ),
    name="null_detector",
)

rate_agent = create_react_agent(
    llm,
    tools=[calculate_completeness_rate],
    prompt=(
        "You are a Completeness Rate Calculator within a multi-agent data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Compute and explain the completeness rate of the dataset — at the column level "
        "and overall. Completeness rate = percentage of non-missing cells. "
        "You do NOT fix anything; you measure and report.\n\n"

        "WORKFLOW:\n"
        "1. Extract the dataset CSV path from the message you received.\n"
        "2. Call calculate_completeness_rate with that exact path.\n"
        "3. Interpret the JSON result and produce a clear completeness report.\n\n"

        "HOW TO WRITE YOUR REPORT:\n"
        "Section 1 — Overall Dataset Completeness:\n"
        "  State the overall completeness rate as a percentage. Classify:\n"
        "  • ≥95%: EXCELLENT  • 80–95%: GOOD  • 60–80%: MODERATE  • <60%: POOR\n\n"
        "Section 2 — Per-Column Completeness:\n"
        "  Sorted table (worst first): Column | Completeness % | Missing Rows.\n"
        "  Highlight columns below key thresholds: <5%, 5–50%, 50–95%, ≥95%.\n\n"
        "Section 3 — Analysis Implications:\n"
        "  For each column below 80% complete, describe the downstream impact on analysis "
        "or machine learning (e.g. skewed aggregations, unreliable groupings, join failures).\n\n"

        "STRICT RULES:\n"
        "- Do NOT modify the dataset. Measurement only.\n"
        "- Be precise — use the tool's output, do not estimate.\n"
        "- Express rates as both percentages AND absolute row counts.\n"
    ),
    name="completeness_rate_calculator",
)

sparse_agent = create_react_agent(
    llm,
    tools=[detect_sparse_columns],
    prompt=(
        "You are a Sparse Column Detection specialist within a multi-agent data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Identify columns that are so empty they provide no analytical value and should be "
        "considered for removal. You do NOT delete anything; you diagnose and recommend.\n\n"

        "WORKFLOW:\n"
        "1. Extract the dataset CSV path from the message you received.\n"
        "2. Call detect_sparse_columns with that exact path.\n"
        "3. Interpret the JSON result and write a detailed sparsity report.\n\n"

        "HOW TO WRITE YOUR REPORT:\n"
        "Section 1 — Removal Candidates (>95% missing):\n"
        "  List every column above 95% empty. For each: name, exact missing %, semantic type, "
        "clear recommendation to DROP. Explain why (waste of memory, confuses models).\n\n"
        "Section 2 — High-Sparsity Columns (50–95% missing):\n"
        "  For each: provide options:\n"
        "  a) Drop if no analytical value  b) Impute if the column is important\n"
        "  c) Add a boolean '[col]_is_missing' indicator to preserve the missingness signal\n\n"
        "Section 3 — Sparsity Impact Assessment:\n"
        "  How many columns remain if all >95% columns are dropped?\n"
        "  Flag columns where the missing pattern is suspicious (same rows always missing "
        "across multiple columns → systematic collection failure).\n\n"

        "STRICT RULES:\n"
        "- Do NOT delete any data. Analysis and recommendation only.\n"
        "- Always state both the absolute count and the percentage.\n"
    ),
    name="sparse_column_detector",
)

# ---------------------------------------------------------------------------
# Worker node wrappers (plain dict return — edges handle routing)
# ---------------------------------------------------------------------------

def _task_only(state: TeamState) -> dict:
    """Return only the first (task) message so agents don't get confused by prior outputs."""
    return {"messages": state["messages"][:1]}


def null_node(state: TeamState) -> dict:
    result = null_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="null_detector"
    )]}


def rate_node(state: TeamState) -> dict:
    result = rate_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="completeness_rate_calculator"
    )]}


def sparse_node(state: TeamState) -> dict:
    result = sparse_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="sparse_column_detector"
    )]}


# ---------------------------------------------------------------------------
# Compiled subgraph — deterministic fixed edges, no LLM supervisor
# ---------------------------------------------------------------------------

completeness_graph = (
    StateGraph(TeamState)
    .add_node("null_detector", null_node)
    .add_node("completeness_rate_calculator", rate_node)
    .add_node("sparse_column_detector", sparse_node)
    .add_edge(START, "null_detector")
    .add_edge("null_detector", "completeness_rate_calculator")
    .add_edge("completeness_rate_calculator", "sparse_column_detector")
    .add_edge("sparse_column_detector", END)
    .compile()
)
