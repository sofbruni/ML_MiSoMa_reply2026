"""
Consistency Validation Team
  Mid-level supervisor : consistency_supervisor
  Worker 1             : format_consistency_checker
  Worker 2             : cross_column_checker
  Worker 3             : duplicate_detector
"""

from typing import Literal
from typing_extensions import TypedDict

from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command

from data_quality.config import get_llm
from data_quality.tools.consistency_tools import (
    check_format_consistency,
    check_cross_column_logic,
    detect_duplicates,
)

llm = get_llm()


class TeamState(MessagesState):
    next: str


# ---------------------------------------------------------------------------
# Worker agents
# ---------------------------------------------------------------------------

format_agent = create_react_agent(
    llm,
    tools=[check_format_consistency],
    prompt=(
        "You are a Format Consistency specialist.\n"
        "When given a dataset path, call check_format_consistency with that path.\n"
        "Report mixed date formats, non-standard period codes, and invalid category values. "
        "Only inspect."
    ),
    name="format_consistency_checker",
)

cross_agent = create_react_agent(
    llm,
    tools=[check_cross_column_logic],
    prompt=(
        "You are a Cross-Column Logic validator.\n"
        "When given a dataset path, call check_cross_column_logic with that path.\n"
        "Report any logical contradictions between columns. Only inspect."
    ),
    name="cross_column_checker",
)

dup_agent = create_react_agent(
    llm,
    tools=[detect_duplicates],
    prompt=(
        "You are a Duplicate Detection specialist.\n"
        "When given a dataset path, call detect_duplicates with that path.\n"
        "Report exact duplicates and near-duplicates on key columns. Only inspect."
    ),
    name="duplicate_detector",
)

# ---------------------------------------------------------------------------
# Worker node wrappers
# ---------------------------------------------------------------------------

def format_node(state: TeamState) -> Command[Literal["consistency_supervisor"]]:
    result = format_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="format_consistency_checker"
        )]},
        goto="consistency_supervisor",
    )


def cross_node(state: TeamState) -> Command[Literal["consistency_supervisor"]]:
    result = cross_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="cross_column_checker"
        )]},
        goto="consistency_supervisor",
    )


def dup_node(state: TeamState) -> Command[Literal["consistency_supervisor"]]:
    result = dup_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="duplicate_detector"
        )]},
        goto="consistency_supervisor",
    )


# ---------------------------------------------------------------------------
# Team supervisor
# ---------------------------------------------------------------------------

def make_supervisor(members: list[str]):
    options = ["FINISH"] + members
    system_prompt = (
        "You are the Consistency Validation supervisor managing: "
        f"{members}. "
        "Route to 'format_consistency_checker', then 'cross_column_checker', "
        "then 'duplicate_detector'. Once all three have reported, respond with FINISH."
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


consistency_supervisor_node = make_supervisor(
    ["format_consistency_checker", "cross_column_checker", "duplicate_detector"]
)

# ---------------------------------------------------------------------------
# Compiled subgraph
# ---------------------------------------------------------------------------

consistency_graph = (
    StateGraph(TeamState)
    .add_node("consistency_supervisor", consistency_supervisor_node)
    .add_node("format_consistency_checker", format_node)
    .add_node("cross_column_checker", cross_node)
    .add_node("duplicate_detector", dup_node)
    .add_edge(START, "consistency_supervisor")
    .compile()
)
