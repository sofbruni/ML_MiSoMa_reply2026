"""
Completeness Analysis Team
  Mid-level supervisor : completeness_supervisor
  Worker 1             : null_detector
  Worker 2             : completeness_rate_calculator
  Worker 3             : sparse_column_detector
"""

from typing import Literal
from typing_extensions import TypedDict

from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command

from data_quality.config import get_llm
from data_quality.tools.completeness_tools import (
    detect_missing_values,
    calculate_completeness_rate,
    detect_sparse_columns,
)

llm = get_llm()


class TeamState(MessagesState):
    next: str


# ---------------------------------------------------------------------------
# Worker agents
# ---------------------------------------------------------------------------

null_agent = create_react_agent(
    llm,
    tools=[detect_missing_values],
    prompt=(
        "You are a Null / Missing Value Detection specialist.\n"
        "When given a dataset path, call detect_missing_values with that path.\n"
        "Report counts and percentages per column. Only inspect."
    ),
    name="null_detector",
)

rate_agent = create_react_agent(
    llm,
    tools=[calculate_completeness_rate],
    prompt=(
        "You are a Completeness Rate Calculator.\n"
        "When given a dataset path, call calculate_completeness_rate with that path.\n"
        "Report the completeness percentage per column and overall. Only inspect."
    ),
    name="completeness_rate_calculator",
)

sparse_agent = create_react_agent(
    llm,
    tools=[detect_sparse_columns],
    prompt=(
        "You are a Sparse Column Detection specialist.\n"
        "When given a dataset path, call detect_sparse_columns with that path.\n"
        "Identify columns that are mostly empty and may be candidates for removal. Only inspect."
    ),
    name="sparse_column_detector",
)

# ---------------------------------------------------------------------------
# Worker node wrappers
# ---------------------------------------------------------------------------

def null_node(state: TeamState) -> Command[Literal["completeness_supervisor"]]:
    result = null_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="null_detector"
        )]},
        goto="completeness_supervisor",
    )


def rate_node(state: TeamState) -> Command[Literal["completeness_supervisor"]]:
    result = rate_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="completeness_rate_calculator"
        )]},
        goto="completeness_supervisor",
    )


def sparse_node(state: TeamState) -> Command[Literal["completeness_supervisor"]]:
    result = sparse_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="sparse_column_detector"
        )]},
        goto="completeness_supervisor",
    )


# ---------------------------------------------------------------------------
# Team supervisor
# ---------------------------------------------------------------------------

def make_supervisor(members: list[str]):
    options = ["FINISH"] + members
    system_prompt = (
        "You are the Completeness Analysis supervisor managing: "
        f"{members}. "
        "Route to 'null_detector' first, then 'completeness_rate_calculator', "
        "then 'sparse_column_detector'. Once all three have reported, respond with FINISH."
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


completeness_supervisor_node = make_supervisor(
    ["null_detector", "completeness_rate_calculator", "sparse_column_detector"]
)

# ---------------------------------------------------------------------------
# Compiled subgraph
# ---------------------------------------------------------------------------

completeness_graph = (
    StateGraph(TeamState)
    .add_node("completeness_supervisor", completeness_supervisor_node)
    .add_node("null_detector", null_node)
    .add_node("completeness_rate_calculator", rate_node)
    .add_node("sparse_column_detector", sparse_node)
    .add_edge(START, "completeness_supervisor")
    .compile()
)
