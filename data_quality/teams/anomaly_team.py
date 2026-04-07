"""
Anomaly Detection Team
  Worker 1 : numerical_outlier_detector
  Worker 2 : categorical_anomaly_detector

Workers run in fixed sequence (deterministic edges — no LLM supervisor needed).
"""

from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, START, END

from data_quality.config import get_llm
from data_quality.tools.anomaly_tools import (
    detect_numerical_outliers,
    detect_categorical_anomalies,
)

llm = get_llm()


class TeamState(MessagesState):
    pass


# ---------------------------------------------------------------------------
# Worker agents
# ---------------------------------------------------------------------------

outlier_agent = create_react_agent(
    llm,
    tools=[detect_numerical_outliers],
    prompt=(
        "You are a Numerical Outlier Detection specialist within a multi-agent data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Apply statistical outlier detection to ALL numeric columns in the dataset and identify "
        "values that are statistically anomalous. Outliers may be genuine extreme values OR "
        "data errors. You detect and characterise them — not remove them.\n\n"

        "WORKFLOW:\n"
        "1. Extract the dataset CSV path from the message you received.\n"
        "2. Call detect_numerical_outliers with that exact path.\n"
        "3. Carefully interpret the JSON result and write a detailed outlier report.\n\n"

        "WHAT THE TOOL DOES:\n"
        "Applies TWO methods to every column where >90% of non-null values parse as float:\n\n"
        "  IQR method (robust to skewed distributions):\n"
        "    Lower fence = Q1 - 1.5 × IQR\n"
        "    Upper fence = Q3 + 1.5 × IQR\n"
        "    Any value outside these fences is an IQR outlier.\n\n"
        "  Z-score method (assumes approximate normality):\n"
        "    Z = (value - mean) / std\n"
        "    Any value with |Z| > 3 is a Z-score outlier.\n\n"

        "HOW TO WRITE YOUR REPORT:\n"
        "For each column with outliers:\n"
        "  - Column name, distribution summary (mean ± std, min, max)\n"
        "  - IQR outlier count and fence values (lower, upper)\n"
        "  - Z-score outlier count\n"
        "  - Top 5 extreme values\n"
        "  - Root cause assessment: (a) legitimate extreme values for this domain, "
        "(b) data entry errors (value 100× the mean), or (c) unit/encoding bugs\n"
        "  - Recommended action: investigate-only, cap at 99th percentile, or flag for review\n"
        "  - Severity: HIGH (>10% of rows), MODERATE (1–10%), LOW (<1%)\n\n"
        "End with a summary table:\n"
        "  Column | IQR Outlier Count | Z-score Outlier Count | Max Value | Severity | Likely Cause\n\n"

        "STRICT RULES:\n"
        "- Do NOT modify the dataset. Detection only.\n"
        "- Report findings for ALL numeric columns the tool returns, not just the worst ones.\n"
        "- Do NOT assume a specific column name exists — work with whatever the tool returns.\n"
    ),
    name="numerical_outlier_detector",
)

cat_anomaly_agent = create_react_agent(
    llm,
    tools=[detect_categorical_anomalies],
    prompt=(
        "You are a Categorical Anomaly Detection specialist within a multi-agent data quality pipeline.\n\n"

        "YOUR ROLE:\n"
        "Identify rare, unexpected, or invalid values in all categorical string columns. "
        "Categorical anomalies degrade model training, corrupt groupby aggregations, and "
        "indicate data entry errors or upstream pipeline bugs. You do NOT fix anything.\n\n"

        "WORKFLOW:\n"
        "1. Extract the dataset CSV path from the message you received.\n"
        "2. Call detect_categorical_anomalies with that exact path.\n"
        "3. Carefully interpret the JSON result and write a detailed categorical anomaly report.\n\n"

        "WHAT THE TOOL CHECKS:\n"
        "Runs on all string columns with ≤50 unique values:\n\n"
        "  Rare values: any value that appears in <0.5% of rows. May be "
        "valid-but-uncommon, placeholder strings not caught earlier ('PENDING', 'TBD', '?'), "
        "or data entry typos.\n\n"
        "  Invalid markers: known invalid values: "
        "{'Unknown', 'n.d.', 'N/A', '?', '//', '-', 'ND'}. These were not converted to NaN "
        "during completeness cleaning and remain as string values.\n\n"

        "HOW TO WRITE YOUR REPORT:\n"
        "Section 1 — Rare Values:\n"
        "  For each column with rare values:\n"
        "  - List ALL rare values, exact counts, and % of total rows\n"
        "  - Classify each as: (a) valid-but-infrequent, (b) probable placeholder/error, "
        "or (c) case/encoding variant of a common value\n"
        "  - Recommend action: keep as-is, map to 'Other', investigate, or convert to NaN\n\n"
        "Section 2 — Remaining Invalid Markers:\n"
        "  List every column still containing invalid marker strings with counts and percentages. "
        "Flag them for re-cleaning.\n\n"
        "Section 3 — Value Distribution Health:\n"
        "  For each analysed column: how many distinct values, dominant category and its %, "
        "any suspicious imbalances (e.g. 99% one value → column has no variance).\n\n"
        "Section 4 — Impact Summary:\n"
        "  How many columns have categorical anomalies, total anomalous values, "
        "which column has the worst anomaly rate.\n\n"

        "STRICT RULES:\n"
        "- Do NOT modify the dataset. Detection only.\n"
        "- Do NOT assume specific column names — work with whatever the tool returns.\n"
        "- Distinguish clearly between rare-but-valid values and probable errors.\n"
    ),
    name="categorical_anomaly_detector",
)

# ---------------------------------------------------------------------------
# Worker node wrappers (plain dict return — edges handle routing)
# ---------------------------------------------------------------------------

def _task_only(state: TeamState) -> dict:
    """Return only the first (task) message so agents don't get confused by prior outputs."""
    return {"messages": state["messages"][:1]}


def outlier_node(state: TeamState) -> dict:
    result = outlier_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="numerical_outlier_detector"
    )]}


def cat_anomaly_node(state: TeamState) -> dict:
    result = cat_anomaly_agent.invoke(_task_only(state))
    return {"messages": [HumanMessage(
        content=result["messages"][-1].content, name="categorical_anomaly_detector"
    )]}


# ---------------------------------------------------------------------------
# Compiled subgraph — deterministic fixed edges, no LLM supervisor
# ---------------------------------------------------------------------------

anomaly_graph = (
    StateGraph(TeamState)
    .add_node("numerical_outlier_detector", outlier_node)
    .add_node("categorical_anomaly_detector", cat_anomaly_node)
    .add_edge(START, "numerical_outlier_detector")
    .add_edge("numerical_outlier_detector", "categorical_anomaly_detector")
    .add_edge("categorical_anomaly_detector", END)
    .compile()
)
