"""
Schema Validation Team
  Worker 1 : data_type_validator
  Worker 2 : naming_convention_checker

Workers run in fixed sequence (deterministic edges — no LLM supervisor needed).
After both workers report, the parent graph node reads all accumulated messages.
"""

from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, START, END

from data_quality.config import get_llm
from data_quality.tools.schema_tools import validate_data_types, check_naming_conventions

llm = get_llm()

# ---------------------------------------------------------------------------
# Worker agents
# ---------------------------------------------------------------------------

dtype_agent = create_react_agent(
    llm,
    tools=[validate_data_types],
    prompt=(
        "You are a Data Type Validation specialist within a multi-agent data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Identify every column in the dataset that contains values of the wrong type — "
        "for example, a column that should hold numbers but contains text, or a date column "
        "with unparseable formats. You do NOT fix anything; you only diagnose.\n\n"

        "WORKFLOW:\n"
        "1. Extract the dataset CSV path from the message you received.\n"
        "2. Call validate_data_types with that exact path.\n"
        "3. Interpret the JSON result carefully and write a structured findings report.\n\n"

        "HOW TO INTERPRET THE TOOL OUTPUT:\n"
        "The tool inspects every column and flags two kinds of contamination:\n"
        "  • 'Mostly numeric but N non-numeric values found' — the column is ≥90% numbers "
        "but contains rogue text. Common causes: currency symbols (€, $, £), unit suffixes, "
        "placeholder strings (N.D., ND, //, ?, n/a), or accidental text entry.\n"
        "  • 'Mostly date/datetime but N unparseable values' — the column is ≥70% recognisable "
        "dates but has entries in incompatible formats: month abbreviations (JAN, Jan, jan), "
        "mixed separators (2024/04/11 vs 11-05-24 vs 24.10.2024), or textual descriptions.\n\n"

        "HOW TO WRITE YOUR REPORT:\n"
        "For each affected column, state:\n"
        "  - Column name and expected semantic type (from the profile in your context)\n"
        "  - Number and percentage of contaminated rows\n"
        "  - Up to 5 concrete examples of the bad values\n"
        "  - Root cause hypothesis (placeholder strings? format mixing? encoding error?)\n"
        "  - Severity: CRITICAL (>10% contaminated), MODERATE (1–10%), MINOR (<1%)\n\n"
        "Provide a summary table:\n"
        "  Column | Expected Type | Contaminated Rows | Severity | Example Bad Values\n\n"
        "If a column has no issues, do NOT mention it — only report problems.\n\n"

        "STRICT RULES:\n"
        "- Do NOT modify the dataset. Inspection only.\n"
        "- Do NOT fabricate issues not present in the tool output.\n"
        "- Columns with 100% missing values are not type violations — skip them.\n"
    ),
    name="data_type_validator",
)

naming_agent = create_react_agent(
    llm,
    tools=[check_naming_conventions],
    prompt=(
        "You are a Column Naming Convention and Duplicate Detection specialist within a "
        "multi-agent data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Identify columns with non-standard names and detect redundant or duplicate columns "
        "that represent the same data under different names. You do NOT fix anything; you diagnose.\n\n"

        "WORKFLOW:\n"
        "1. Extract the dataset CSV path from the message you received.\n"
        "2. Call check_naming_conventions with that exact path.\n"
        "3. Interpret the JSON result and produce a detailed findings report.\n\n"

        "NAMING CONVENTION RULES (standard data-engineering snake_case):\n"
        "  • All lowercase — 'TOTAL AMOUNT' violates this rule\n"
        "  • Words separated by underscores, not spaces — 'col name ext' violates this\n"
        "  • Must start with a letter, not a digit — '2col_name' violates this\n"
        "  • No special characters: %, @, !, # — 'col%code' violates this\n"
        "  • No hyphens — 'col-name' violates this (use underscores)\n\n"

        "DUPLICATE COLUMN DETECTION:\n"
        "The tool uses two-step detection: (a) normalized-name substring matching, then "
        "(b) Jaccard value overlap > 80%. A column is flagged as a probable duplicate when "
        "its name contains or is contained within another column's name AND most of its values "
        "match. Common patterns:\n"
        "  • '[label_col]' vs '[LABEL COL]' — same data, different capitalisation/spacing\n"
        "  • '[col]' vs '2[col]' vs '[col] ext' — same concept, varied prefixes/suffixes\n"
        "  • '[short_name]' vs '[LONG DESCRIPTIVE NAME]' — one is a renamed copy of the other\n\n"

        "HOW TO WRITE YOUR REPORT:\n"
        "Section 1 — Naming Violations:\n"
        "  List every column with naming issues. For each: the column name, the violation(s) "
        "detected, and the recommended corrected name.\n"
        "  Summary table: Column | Violation(s) | Recommended Name.\n\n"
        "Section 2 — Duplicate / Redundant Columns:\n"
        "  For each flagged duplicate pair: identify the probable original vs the redundant copy, "
        "explain the evidence (name similarity + value overlap percentage), and recommend "
        "which column to keep and which to drop.\n"
        "  Table: Redundant Column | Likely Original | Value Overlap | Action.\n\n"
        "Section 3 — Impact Summary:\n"
        "  State the total count of naming violations, total duplicate pairs found, and the "
        "overall naming quality of the dataset.\n\n"

        "STRICT RULES:\n"
        "- Do NOT modify the dataset. Inspection only.\n"
        "- Report ALL naming violations, even minor ones.\n"
        "- If no issues are found, explicitly state 'No naming violations or duplicate columns detected.'\n"
    ),
    name="naming_convention_checker",
)

# ---------------------------------------------------------------------------
# Worker node wrappers (plain dict return — edges handle routing)
# ---------------------------------------------------------------------------

class TeamState(MessagesState):
    pass


def _task_only(state: TeamState) -> dict:
    """Return only the first (task) message so agents don't get confused by prior outputs."""
    return {"messages": state["messages"][:1]}


def dtype_node(state: TeamState) -> dict:
    result = dtype_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="data_type_validator"
    )]}


def naming_node(state: TeamState) -> dict:
    result = naming_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="naming_convention_checker"
    )]}


# ---------------------------------------------------------------------------
# Compiled subgraph — deterministic fixed edges, no LLM supervisor
# ---------------------------------------------------------------------------

schema_graph = (
    StateGraph(TeamState)
    .add_node("data_type_validator", dtype_node)
    .add_node("naming_convention_checker", naming_node)
    .add_edge(START, "data_type_validator")
    .add_edge("data_type_validator", "naming_convention_checker")
    .add_edge("naming_convention_checker", END)
    .compile()
)
