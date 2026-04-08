# Data Quality Flow Visual Guide

This guide explains the pipeline visually, with focus on:
- control flow
- where LLM is used
- where deterministic logic is enforced
- how artifacts evolve (`v1`..`v4`)

## Legend
- LLM node: model reasoning/generation
- Deterministic node: Python/pandas/rules-only
- Hybrid node: deterministic constraints + LLM decision support

## 1) End-to-End Flow
```mermaid
flowchart TD
    A[Input CSV] --> B[Profiler\nDeterministic]
    B --> C[Semantic Enricher\nLLM]
    C --> D[Top Supervisor\nHybrid]

    D --> E[Schema Team]
    D --> F[Completeness Team]
    D --> G[Consistency Team]
    D --> H[Anomaly Team]
    D --> I[Remediation Team]

    E --> D
    F --> D
    G --> D
    H --> D
    I --> J[Outputs\nv4 + JSON + MD]
```

## 2) Hybrid Supervisor Decision Logic
```mermaid
flowchart TD
    S[Supervisor cycle] --> R1{Iteration trigger?}
    R1 -- Yes --> RI[Deterministic iteration route]
    R1 -- No --> R2[Compute eligible teams\nDeterministic]

    R2 --> R3{Eligible = FINISH?}
    R3 -- Yes --> END[Finish]
    R3 -- No --> R4{Borderline/ambiguous case?}

    R4 -- No --> RD[Deterministic top choice]
    R4 -- Yes --> RL[Ask LLM for route\n(next/reason/confidence)]

    RL --> RC{Valid + confidence >= threshold?}
    RC -- Yes --> RL2[Use LLM choice]
    RC -- No --> RD

    RI --> LOG[Append supervisor_decisions log]
    RD --> LOG
    RL2 --> LOG
    LOG --> NX[Go to selected team]
```

Guardrails always apply:
- schema first
- remediation last
- max 2 iterations
- LLM can only select from eligible teams

## 3) Team Internals

### Schema
```mermaid
flowchart LR
    A1[data_type_validator\nLLM+tool] --> A2[naming_convention_checker\nLLM+tool]
    A2 --> A3[apply_schema_fixes\nDeterministic\nwrite v1]
```

### Completeness
```mermaid
flowchart LR
    B1[null_detector\nLLM+tool] --> B2[completeness_rate_calculator\nLLM+tool]
    B2 --> B3[sparse_column_detector\nLLM+tool]
    B3 --> B4[apply_completeness_fixes\nDeterministic\nwrite v2]
```

### Consistency
```mermaid
flowchart LR
    C1[format_consistency_checker\nLLM+tool] --> C2[cross_column_checker\nLLM+tool]
    C2 --> C3[duplicate_detector\nLLM+tool]
    C3 --> C4[apply_consistency_fixes\nDeterministic\nwrite v3]
```

### Anomaly
```mermaid
flowchart LR
    D1[numerical_outlier_detector\nLLM+tool] --> D2[categorical_anomaly_detector\nLLM+tool]
    D2 --> D3[Findings only\nNo write]
```

### Remediation
```mermaid
flowchart LR
    E1[correction_suggester\nLLM+tool] --> E2[reliability_scorer\nLLM+tool]
    E2 --> E3[apply_remediation_fixes\nDeterministic\nwrite v4]
    E3 --> E4[Report builder\nDeterministic + LLM narrative]
```

## 4) LLM vs Deterministic Map

| Component | Type | Why |
|---|---|---|
| Profiler (`create_dataset_profile`) | Deterministic | Fast, stable profiling |
| Semantic Enricher | LLM | semantic hints from names/samples |
| Top Supervisor | Hybrid | adaptive routing with strict guardrails |
| Team workers | LLM | richer diagnostics and explanations |
| `apply_*_fixes` | Deterministic | reproducible data transformations |
| Supervisor narrative (report) | LLM | concise presentation-ready explanation |
| Report serialization | Deterministic | stable artifact generation |

## 5) Artifact Evolution
```mermaid
flowchart LR
    O[original.csv] --> V1[v1 schema]
    V1 --> V2[v2 completeness if executed]
    V2 --> V3[v3 consistency if executed]
    V3 --> V4[v4 remediation]
    V4 --> R1[quality_report.json]
    V4 --> R2[quality_report.md]
```

## 6) Reporting Additions (Supervisor Explainability)
Reports now include:
- `supervisor_decisions`: per-step selected team, source (`deterministic`/`llm`), confidence, reason
- `supervisor_narrative`: short LLM-generated paragraph for slide-ready storytelling

This makes orchestration explainable to both technical and non-technical reviewers.
