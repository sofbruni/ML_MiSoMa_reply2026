"""Top-level graph wiring for the data quality pipeline."""

from langgraph.graph import START, StateGraph

from data_quality.orchestration.nodes import (
    call_anomaly_team,
    call_completeness_team,
    call_consistency_team,
    call_remediation_team,
    call_schema_team,
    run_profiler,
    run_semantic_enricher,
)
from data_quality.orchestration.supervisor import smart_supervisor_node
from data_quality.state import DataQualityState


def build_graph():
    """Build and compile the top-level hierarchical graph."""
    builder = StateGraph(DataQualityState)

    builder.add_node("profiler", run_profiler)
    builder.add_node("semantic_enricher", run_semantic_enricher)
    builder.add_node("top_supervisor", smart_supervisor_node)
    builder.add_node("schema_team", call_schema_team)
    builder.add_node("completeness_team", call_completeness_team)
    builder.add_node("consistency_team", call_consistency_team)
    builder.add_node("anomaly_team", call_anomaly_team)
    builder.add_node("remediation_team", call_remediation_team)

    builder.add_edge(START, "profiler")
    builder.add_edge("profiler", "semantic_enricher")
    builder.add_edge("semantic_enricher", "top_supervisor")

    return builder.compile()


graph = build_graph()
