# Data Quality Multi-Agent System (AGENTS.md)

## Purpose
This document explains the `data_quality` architecture for reviewers:
- how the pipeline is orchestrated
- what each team does
- where LLM is used vs deterministic logic
- how routing and iteration work
- what artifacts/reports are produced

## Architecture Overview
Top-level graph: `data_quality/graph.py`

Execution stages:
1. `profiler` (deterministic)
2. `semantic_enricher` (LLM-assisted metadata enrichment)
3. `top_supervisor` (hybrid: deterministic guardrails + LLM routing in ambiguous cases)
4. Specialist teams (`schema`, `completeness`, `consistency`, `anomaly`, `remediation`)
5. Final outputs (`v4`, JSON report, Markdown report)

Each team is a deterministic subgraph with fixed worker order (no inner LLM supervisor).

## Core Design
The system intentionally separates responsibilities:
- LLMs: interpretation, diagnostics, prioritization, narrative quality
- deterministic code: routing constraints, file mutation, reproducible fixes, persistence

This gives both agentic behavior and production-safe repeatability.

## Shared State (`DataQualityState`)
Defined in `data_quality/state.py`.

Main fields:
- dataset paths: `original_dataset_path`, `working_dataset_path`
- profile: `dataset_profile`
- flow control: `completed_teams`, `skipped_teams`, `next`
- adaptive iteration: `iteration_count`, `rows_removed_last_team`, `types_changed_count`
- telemetry: `supervisor_decisions`

## Supervisor Model (Hybrid)
The supervisor is not purely deterministic anymore.

### Deterministic guardrails (always enforced)
- `schema_team` must be first
- `remediation_team` must be last
- max iterations = 2
- LLM can only pick among currently eligible teams
- if LLM output is invalid/low-confidence, fallback to deterministic choice

### LLM involvement (agentic routing)
In borderline/ambiguous contexts (e.g. near thresholds or multiple eligible teams), the supervisor asks the LLM for a structured route decision:
- `next`
- `reason`
- `confidence`
- `expected_impact`

If confidence is above threshold, LLM choice is adopted.

### Iteration triggers
- after schema: if `types_changed_count > 5` -> re-run consistency
- after consistency: if `rows_removed_last_team > 10%` of profiled rows -> re-run completeness

## Team Responsibilities

### 1) Schema Team
File: `teams/schema_team.py`
Workers:
- `data_type_validator`
- `naming_convention_checker`

Tools/fixes: `tools/schema_tools.py`
- diagnosis tools + `apply_schema_fixes(...)`
- writes `*_v1.csv`
- emits `types_changed_count`

### 2) Completeness Team
File: `teams/completeness_team.py`
Workers:
- `null_detector`
- `completeness_rate_calculator`
- `sparse_column_detector`

Tools/fixes: `tools/completeness_tools.py`
- diagnosis tools + `apply_completeness_fixes(...)`
- writes `*_v2.csv` (or iterative `*_v2_iterX.csv`)

### 3) Consistency Team
File: `teams/consistency_team.py`
Workers:
- `format_consistency_checker`
- `cross_column_checker`
- `duplicate_detector`

Tools/fixes: `tools/consistency_tools.py`
- diagnosis tools + `apply_consistency_fixes(...)`
- writes `*_v3.csv` (or iterative `*_v3_iterX.csv`)
- emits `rows_removed`

### 4) Anomaly Team
File: `teams/anomaly_team.py`
Workers:
- `numerical_outlier_detector`
- `categorical_anomaly_detector`

Tools: `tools/anomaly_tools.py`
- findings only, no CSV write

### 5) Remediation Team
File: `teams/remediation_team.py`
Workers:
- `correction_suggester`
- `reliability_scorer`

Tools/fixes: `tools/remediation_tools.py`
- generates confidence-scored suggestions
- applies high-confidence low-risk fixes via `apply_remediation_fixes(...)`
- writes final `*_v4.csv`
- computes score + roadmap + expected post-fix score

## Team Interaction Model
Teams do not directly call each other.
All coordination happens through top-graph state and artifact handoff:
- team reads `working_dataset_path` + profile
- team emits findings message
- deterministic fixer updates working file
- supervisor decides next step

## Artifacts
For `data/spesa.csv`, typical outputs:
- `data/spesa_v1.csv`
- `data/spesa_v2.csv` (if completeness runs)
- `data/spesa_v3.csv` (if consistency runs)
- `data/spesa_v4.csv` (final)
- `data/spesa_quality_report.json`
- `data/spesa_quality_report.md`

Iterative extras can appear (`*_iter1`, `*_iter2`).

## Reporting Outputs
Final reports include:
- executed/skipped teams
- iteration count
- `supervisor_decisions` with source (`deterministic` / `llm`), confidence, and reason
- `supervisor_narrative` (short LLM-generated summary for presentation)
- findings by team
- applied vs skipped remediation fixes
- reliability score + grade + roadmap

## Why this is coherent
This architecture demonstrates:
- adaptive orchestration (not fixed pipeline)
- multi-agent specialization
- controlled LLM reasoning with hard safety constraints
- actionable remediation and explainable reporting
