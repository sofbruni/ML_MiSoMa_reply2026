"""
Remediation Team
  Mid-level supervisor : remediation_supervisor
  Worker 1             : correction_suggester  (LLM-driven, uses generate_correction_suggestions)
  Worker 2             : reliability_scorer    (LLM-driven, uses calculate_reliability_score)
"""

from typing import Literal
from typing_extensions import TypedDict

from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command

from data_quality.config import get_llm
from data_quality.tools.remediation_tools import (
    generate_correction_suggestions,
    calculate_reliability_score,
)

llm = get_llm()


class TeamState(MessagesState):
    next: str


# ---------------------------------------------------------------------------
# Worker agents
# ---------------------------------------------------------------------------

suggester_agent = create_react_agent(
    llm,
    tools=[generate_correction_suggestions],
    prompt=(
        "You are a Correction Suggestions specialist.\n"
        "You will receive a JSON string containing all data quality findings from the previous teams.\n"
        "Call generate_correction_suggestions with that JSON string.\n"
        "Present the suggestions clearly, grouped by issue type."
    ),
    name="correction_suggester",
)

scorer_agent = create_react_agent(
    llm,
    tools=[calculate_reliability_score],
    prompt=(
        "You are a Reliability Scorer.\n"
        "You will receive a JSON string containing all data quality findings.\n"
        "Call calculate_reliability_score with that JSON string.\n"
        "Report the final score, grade, and the deduction breakdown clearly."
    ),
    name="reliability_scorer",
)

# ---------------------------------------------------------------------------
# Worker node wrappers
# ---------------------------------------------------------------------------

def suggester_node(state: TeamState) -> Command[Literal["remediation_supervisor"]]:
    result = suggester_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="correction_suggester"
        )]},
        goto="remediation_supervisor",
    )


def scorer_node(state: TeamState) -> Command[Literal["remediation_supervisor"]]:
    result = scorer_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="reliability_scorer"
        )]},
        goto="remediation_supervisor",
    )


# ---------------------------------------------------------------------------
# Team supervisor
# ---------------------------------------------------------------------------

def make_supervisor(members: list[str]):
    options = ["FINISH"] + members
    system_prompt = (
        "You are the Remediation supervisor managing: "
        f"{members}. "
        "Route to 'correction_suggester' first, then 'reliability_scorer'. "
        "Once both have delivered their results, respond with FINISH."
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


remediation_supervisor_node = make_supervisor(
    ["correction_suggester", "reliability_scorer"]
)

# ---------------------------------------------------------------------------
# Compiled subgraph
# ---------------------------------------------------------------------------

remediation_graph = (
    StateGraph(TeamState)
    .add_node("remediation_supervisor", remediation_supervisor_node)
    .add_node("correction_suggester", suggester_node)
    .add_node("reliability_scorer", scorer_node)
    .add_edge(START, "remediation_supervisor")
    .compile()
)
