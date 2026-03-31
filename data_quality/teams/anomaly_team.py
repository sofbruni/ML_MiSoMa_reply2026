"""
Anomaly Detection Team
  Mid-level supervisor : anomaly_supervisor
  Worker 1             : numerical_outlier_detector
  Worker 2             : categorical_anomaly_detector
"""

from typing import Literal
from typing_extensions import TypedDict

from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command

from data_quality.config import get_llm
from data_quality.tools.anomaly_tools import (
    detect_numerical_outliers,
    detect_categorical_anomalies,
)

llm = get_llm()


class TeamState(MessagesState):
    next: str


# ---------------------------------------------------------------------------
# Worker agents
# ---------------------------------------------------------------------------

outlier_agent = create_react_agent(
    llm,
    tools=[detect_numerical_outliers],
    prompt=(
        "You are a Univariate Outlier Detection specialist.\n"
        "When given a dataset path, call detect_numerical_outliers with that path.\n"
        "Report statistical outliers in the 'spesa' column using IQR and Z-score methods. "
        "Only inspect."
    ),
    name="numerical_outlier_detector",
)

cat_anomaly_agent = create_react_agent(
    llm,
    tools=[detect_categorical_anomalies],
    prompt=(
        "You are a Categorical Anomaly Detection specialist.\n"
        "When given a dataset path, call detect_categorical_anomalies with that path.\n"
        "Flag rare or unexpected categorical values. Only inspect."
    ),
    name="categorical_anomaly_detector",
)

# ---------------------------------------------------------------------------
# Worker node wrappers
# ---------------------------------------------------------------------------

def outlier_node(state: TeamState) -> Command[Literal["anomaly_supervisor"]]:
    result = outlier_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="numerical_outlier_detector"
        )]},
        goto="anomaly_supervisor",
    )


def cat_anomaly_node(state: TeamState) -> Command[Literal["anomaly_supervisor"]]:
    result = cat_anomaly_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="categorical_anomaly_detector"
        )]},
        goto="anomaly_supervisor",
    )


# ---------------------------------------------------------------------------
# Team supervisor
# ---------------------------------------------------------------------------

def make_supervisor(members: list[str]):
    options = ["FINISH"] + members
    system_prompt = (
        "You are the Anomaly Detection supervisor managing: "
        f"{members}. "
        "Route to 'numerical_outlier_detector' first, then 'categorical_anomaly_detector'. "
        "Once both have reported, respond with FINISH."
    )

    class Router(TypedDict):
        next: Literal[*options]  # type: ignore[valid-type]

    def supervisor_node(state: TeamState) -> Command[Literal[*members, "__end__"]]:  # type: ignore[valid-type]
        messages = [{"role": "system", "content": system_prompt}] + state["messages"]
        response = llm.with_structured_output(Router).invoke(messages)
        goto = response["next"]
        if goto == "FINISH":
            goto = END
        return Command(goto=goto, update={"next": goto})

    return supervisor_node


anomaly_supervisor_node = make_supervisor(
    ["numerical_outlier_detector", "categorical_anomaly_detector"]
)

# ---------------------------------------------------------------------------
# Compiled subgraph
# ---------------------------------------------------------------------------

anomaly_graph = (
    StateGraph(TeamState)
    .add_node("anomaly_supervisor", anomaly_supervisor_node)
    .add_node("numerical_outlier_detector", outlier_node)
    .add_node("categorical_anomaly_detector", cat_anomaly_node)
    .add_edge(START, "anomaly_supervisor")
    .compile()
)
