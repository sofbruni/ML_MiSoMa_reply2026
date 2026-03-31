"""
Entry point for the Data Quality Multi-Agent System.

Usage:
    python main.py

Set your API key either in a .env file:
    GOOGLE_API_KEY=your_key_here

Or export it before running:
    export GOOGLE_API_KEY=your_key_here
"""

import os
from pathlib import Path
from langchain_core.messages import HumanMessage
from data_quality.graph import graph

DATASET_PATH = (
    r"C:\Users\sebas\OneDrive - LUISS Libera Università Internazionale degli Studi Sociali Guido Carli"
    r"\Documenti\GitHub\ML_MiSoMa_reply2026\data\spesa.csv"
)


def run_pipeline(dataset_path: str) -> dict:
    print("=" * 70)
    print("  DATA QUALITY MULTI-AGENT SYSTEM")
    print("=" * 70)
    print(f"  Input dataset : {Path(dataset_path).name}")
    print(f"  Full path     : {dataset_path}")
    print("=" * 70)
    print()

    initial_state = {
        "messages": [
            HumanMessage(
                content=(
                    f"Please perform a full data quality analysis on the dataset at: {dataset_path}\n"
                    "Run all 5 quality teams in order: Schema -> Completeness -> "
                    "Consistency -> Anomaly -> Remediation.\n"
                    "After each team, apply the appropriate fixes to the working CSV "
                    "before proceeding to the next team."
                )
            )
        ],
        "original_dataset_path": dataset_path,
        "working_dataset_path": dataset_path,
    }

    for step in graph.stream(initial_state, stream_mode="updates"):
        for node_name, node_output in step.items():
            print("-" * 60)
            print(f"  Node: {node_name}")
            print("-" * 60)
            if "messages" in node_output and node_output["messages"]:
                last_msg = node_output["messages"][-1]
                raw = getattr(last_msg, "content", str(last_msg))
                if isinstance(raw, list):
                    content = "\n".join(
                        p["text"] for p in raw
                        if isinstance(p, dict) and p.get("type") == "text"
                    )
                else:
                    content = str(raw)
                print(content)
            if "working_dataset_path" in node_output:
                print(f"\n  Working CSV updated -> {node_output['working_dataset_path']}")
            print()

    print("=" * 70)
    print("  PIPELINE COMPLETE")
    print("  Reports saved to data/quality_report.json and data/quality_report.md")
    print("=" * 70)

    return {"status": "completed", "original_path": dataset_path}


if __name__ == "__main__":
    if not os.getenv("GOOGLE_API_KEY"):
        print("WARNING: GOOGLE_API_KEY not set. Please set it before running.")
        print("  export GOOGLE_API_KEY=your_key_here")
        print("  or add it to a .env file in the project root.\n")

    run_pipeline(DATASET_PATH)
