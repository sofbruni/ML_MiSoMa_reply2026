"""Shared helpers for deterministic team subgraphs."""

from langchain_core.messages import HumanMessage
from langgraph.graph import MessagesState


class TeamState(MessagesState):
    """Standard team-local state container."""


def task_only(state: TeamState) -> dict:
    """Return only the first user task message for worker invocation."""
    return {"messages": state["messages"][:1]}


def run_worker(agent, state: TeamState, message_name: str) -> dict:
    """Invoke a worker agent and wrap its final message as `HumanMessage`."""
    result = agent.invoke(task_only(state))
    return {"messages": [HumanMessage(content=result["messages"][-1].content, name=message_name)]}
