"""
Entry point for the Data Quality Multi-Agent System.

Usage:
    python main.py                                  # defaults to data/spesa.csv
    python main.py data/attivazioniCessazioni.csv   # any CSV
    python main.py /absolute/path/to/dataset.csv

Set your API key either in a .env file:
    GOOGLE_API_KEY=your_key_here

Or export it before running:
    export GOOGLE_API_KEY=your_key_here
"""

import argparse
import os
import sys
from pathlib import Path
from langchain_core.messages import HumanMessage
from data_quality.graph import graph

# Force UTF-8 output on Windows (prevents UnicodeEncodeError for ≥, →, etc.)
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def _resolve_dataset_path() -> str:
    parser = argparse.ArgumentParser(description="Data Quality Multi-Agent System")
    parser.add_argument(
        "dataset",
        nargs="?",
        default=None,
        help="Path to the CSV dataset to analyse (default: attivazioniCessazioni.csv in data/ folder)",
    )
    args = parser.parse_args()

    if args.dataset:
        path = Path(args.dataset)
    else:
        path = Path(__file__).parent / "data" / "spesa.csv"

    if not path.exists():
        print(f"ERROR: Dataset not found at '{path}'")
        sys.exit(1)

    return str(path.resolve())


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
        "dataset_profile": {},   # populated by profiler node before pipeline starts
        "completed_teams": [],
        "skipped_teams": [],
        "last_completed_team": "",
        "iteration_count": 0,
        "rows_removed": 0,
        "rows_removed_last_team": 0,
        "types_changed_count": 0,
        "supervisor_decisions": [],
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
    stem = Path(dataset_path).stem
    print(f"  Reports saved to data/{stem}_quality_report.json and data/{stem}_quality_report.md")
    print("=" * 70)

    return {"status": "completed", "original_path": dataset_path}


if __name__ == "__main__":
    if not os.getenv("GOOGLE_API_KEY"):
        print("WARNING: GOOGLE_API_KEY not set. Please set it before running.")
        print("  export GOOGLE_API_KEY=your_key_here")
        print("  or add it to a .env file in the project root.\n")

    dataset_path = _resolve_dataset_path()
    run_pipeline(dataset_path)
