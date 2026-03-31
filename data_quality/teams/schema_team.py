"""
Schema Validation Team
  Mid-level supervisor  : schema_supervisor
  Worker 1              : data_type_validator
  Worker 2              : naming_convention_checker

After both workers report findings, apply_schema_fixes() is called by the
parent graph node (call_schema_team) to produce the next working CSV.
"""

from typing import Literal
from typing_extensions import TypedDict

from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command

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
        "You are a Data Type Validation specialist.\n"
        "When given a dataset path, call validate_data_types with that path.\n"
        "Report the findings clearly. Do not perform any fixes — only inspect."
    ),
    name="data_type_validator",
)

naming_agent = create_react_agent(
    llm,
    tools=[check_naming_conventions],
    prompt=(
        "You are a Naming Convention Check specialist.\n"
        "When given a dataset path, call check_naming_conventions with that path.\n"
        "Report all naming violations and duplicate columns found. Only inspect, do not fix."
    ),
    name="naming_convention_checker",
)

# ---------------------------------------------------------------------------
# Worker node wrappers
# ---------------------------------------------------------------------------

class TeamState(MessagesState):
    next: str


def dtype_node(state: TeamState) -> Command[Literal["schema_supervisor"]]:
    result = dtype_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="data_type_validator"
        )]},
        goto="schema_supervisor",
    )


def naming_node(state: TeamState) -> Command[Literal["schema_supervisor"]]:
    result = naming_agent.invoke(state)
    return Command(
        update={"messages": [HumanMessage(
            content=result["messages"][-1].content, name="naming_convention_checker"
        )]},
        goto="schema_supervisor",
    )


# ---------------------------------------------------------------------------
# Team supervisor
# ---------------------------------------------------------------------------

def make_supervisor(members: list[str]):
    options = ["FINISH"] + members
    system_prompt = (
        "You are the Schema Validation supervisor managing these workers: "
        f"{members}. "
        "First send the task to 'data_type_validator', then to 'naming_convention_checker'. "
        "Once both have reported their findings, respond with FINISH."
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


schema_supervisor_node = make_supervisor(["data_type_validator", "naming_convention_checker"])

# ---------------------------------------------------------------------------
# Compiled subgraph
# ---------------------------------------------------------------------------

schema_graph = (
    StateGraph(TeamState)
    .add_node("schema_supervisor", schema_supervisor_node)
    .add_node("data_type_validator", dtype_node)
    .add_node("naming_convention_checker", naming_node)
    .add_edge(START, "schema_supervisor")
    .compile()
)
