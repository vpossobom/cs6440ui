from __future__ import annotations

from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from nodes.ingest import ingest_node
from nodes.mapping import generate_mapping_node
from nodes.schema import analyze_schema_node


class PipelineState(TypedDict, total=False):
    file_path: str
    target_resource_type: str
    dataframe: Any
    metadata: dict[str, Any]
    schema_summary: dict[str, Any]
    sample_rows: list[dict[str, Any]]
    fhir_mapping: dict[str, Any]
    fhir_bundle: dict[str, Any] | None
    errors: list[dict[str, Any]]


def build_graph():
    graph = StateGraph(PipelineState)

    graph.add_node("ingest", ingest_node)
    graph.add_node("analyze_schema", analyze_schema_node)
    graph.add_node("generate_mapping", generate_mapping_node)

    graph.add_edge(START, "ingest")
    graph.add_edge("ingest", "analyze_schema")
    graph.add_edge("analyze_schema", "generate_mapping")
    graph.add_edge("generate_mapping", END)

    return graph.compile()


pipeline_graph = build_graph()


def run_pipeline(file_path: str, target_resource_type: str = "Patient") -> PipelineState:
    initial_state: PipelineState = {
        "file_path": file_path,
        "target_resource_type": target_resource_type,
        "fhir_bundle": None,
        "errors": [],
    }
    return pipeline_graph.invoke(initial_state)
