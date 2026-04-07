# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NoiPA Multi-Agent Data Quality System — a hierarchical LangGraph pipeline that inspects and cleans Italian Public Administration (NoiPA) payroll CSV datasets. Built for LUISS × Reply university project (2026).

## Setup & Running

```bash
# Install dependencies
pip install -r requirements.txt

# Set API key (required)
export GOOGLE_API_KEY="your_key_here"

# Run the pipeline
python main.py
```

The pipeline processes `data/spesa.csv` by default (hardcoded Windows path in `main.py`). Output files go to `data/`: versioned CSVs (`_v1.csv` through `_v4.csv`), `quality_report.json`, and `quality_report.md`.

## Architecture

### Multi-Agent Hierarchy

```
Top Supervisor (graph.py)
├── schema_team       → validates types, naming → _v1.csv
├── completeness_team → detects/fills nulls     → _v2.csv
├── consistency_team  → normalizes formats      → _v3.csv
├── anomaly_team      → flags outliers          → (no CSV change)
└── remediation_team  → scores + suggestions   → _v4.csv (final)
```

Teams run **sequentially** in fixed order. Each team is a compiled LangGraph subgraph with its own supervisor and worker agents.

### Key Files

| File | Purpose |
|------|---------|
| `main.py` | CLI entry point, streams graph execution |
| `data_quality/graph.py` | Top-level supervisor + all team wrapper nodes + report generation |
| `data_quality/state.py` | `DataQualityState` (pipeline) and `TeamState` (per-team) |
| `data_quality/config.py` | LLM config (`MODEL_NAME`, `GOOGLE_API_KEY`, `PLACEHOLDER_VALUES`) |
| `data_quality/teams/*.py` | 5 team subgraphs with ReAct worker agents |
| `data_quality/tools/*.py` | Tool functions (inspection) + fix functions (called by graph nodes) |
| `src/multi_agent.py` | Groq-based reference implementation (not part of main pipeline) |

### Pattern: Tools vs. Fixes

Each `tools/*.py` file has two categories of functions:
- **LLM-callable tools** (decorated with `@tool`): called by worker agents to inspect data, return findings as text
- **Fix functions** (plain Python): called directly by graph nodes in `graph.py` to modify the working CSV

### LLM

Uses `gemini-3.1-flash-lite-preview` (via `langchain-google-genai`). Workers use ReAct pattern; team supervisors use structured output for routing. Config in `data_quality/config.py`.

### Reliability Score Formula

```
score = schema(20%) + completeness(25%) + consistency(30%) + anomaly(25%)
```
Grade: A ≥ 90, B ≥ 75, C ≥ 60, D ≥ 45, F < 45

### Dataset Columns (NoiPA payroll)

`rata`, `spesa`, `ente`, `cod_imposta`, `tipo_imposta`, `area_geografica`, `cod_tipoimposta`, `descrizione`, `aggregation_time`

Expected mappings: `cod_tipoimposta` {1→Netto, 2→Erariali, 3→Previdenziali, 4→Varie}

---

## Improvement Plan: Dataset-Agnostic Refactor

### Context
The pipeline works for `spesa.csv` but is hardcoded to NoiPA column names and business rules. The Reply–LUISS project requires it to work on **any CSV dataset**. The LangGraph hierarchy is already correct — only the tools and fix functions need to be made generic via a runtime dataset profile.

### Root Cause: Hardcoding Sites

| File | Hardcoded assumption |
|------|---------------------|
| `schema_tools.py` | validates only `rata`, `spesa`, `ente`, `cod_imposta`; known_dupes dict |
| `consistency_tools.py` | `VALID_TIPO_IMPOSTA`, `RATA_PATTERN`, `MONTH_IT`, `cod_tipoimposta` mapping, key cols `(rata, ente, cod_imposta)` |
| `anomaly_tools.py` | outlier detection only on `spesa`; categorical check only on 4 hardcoded columns |
| `completeness_tools.py` | `apply_completeness_fixes` fills hardcoded column lists |
| `main.py` | absolute Windows path to `spesa.csv` |

### Solution: Dataset Profiler + Profile-Driven Tools

Add a **profiler node** that runs once before the top supervisor, detects column semantic types via pandas heuristics, and stores the result in state. Every tool and fix function reads from this profile instead of hardcoded names.

#### Step 1 — `data_quality/state.py`
Add one field:
```python
class DataQualityState(MessagesState):
    original_dataset_path: str
    working_dataset_path: str
    dataset_profile: dict   # NEW
    next: str
```

#### Step 2 — `data_quality/tools/profiler.py` (CREATE NEW)
Pure-pandas, no LLM. Detects semantic type per column:
1. **boolean** — ≤3 unique values, all in `{true/false/yes/no/1/0}`
2. **identifier** — `cardinality_ratio > 0.95`
3. **numeric** — >90% of non-null values parse as float
4. **date** — >70% parse via `pd.to_datetime(infer_datetime_format=True)` and not numeric
5. **categorical** — `cardinality_ratio < 0.05`
6. **text** — fallback

Returns: `{row_count, column_count, columns: {col: {dtype, semantic_type, null_pct, cardinality, cardinality_ratio, sample_values}}}`

Also auto-detects CSV separator via `csv.Sniffer`.

#### Step 3 — `data_quality/graph.py`
- Add `run_profiler` node; wire `START → profiler → top_supervisor`
- Update `_team_initial_message(path, task, profile)` to embed profile JSON
- Pass `profile = state["dataset_profile"]` to all team calls and fix functions
- Inject `column_count` + `row_count` into remediation findings payload

#### Step 4 — `data_quality/tools/schema_tools.py`
- `validate_data_types`: iterate ALL columns dynamically; flag numeric contamination and date contamination
- `check_naming_conventions`: replace hardcoded `known_dupes` with Jaccard name+value similarity (>80% value overlap)
- `apply_schema_fixes(path, out, profile)`: drop duplicates by normalized-name dedup; rename hyphenated cols; strip currency symbols from numeric cols; coerce types from profile

#### Step 5 — `data_quality/tools/completeness_tools.py`
- 3 `@tool` functions already generic — no changes
- `apply_completeness_fixes(path, out, profile)`: derive numeric cols from profile instead of hardcoded list; remove hardcoded sparse candidates

#### Step 6 — `data_quality/tools/consistency_tools.py` (LARGEST CHANGE)
- Remove `VALID_TIPO_IMPOSTA`, `RATA_PATTERN`, `MONTH_IT` constants
- `check_format_consistency`: detect ALL date-like columns dynamically via 10 regex patterns; flag mixed formats and case/whitespace inconsistencies in categorical cols
- `check_cross_column_logic`: auto-detect code-label pairs (numeric col cardinality 2–15 paired with categorical col); flag negative values in amount-like columns by keyword matching
- `detect_duplicates`: auto-detect key cols by cardinality ratio [0.05, 0.95] and avg value length <30
- `apply_consistency_fixes(path, out, profile)`: normalize date cols via `pd.to_datetime`; strip whitespace from categorical cols; drop exact duplicates. Remove all NoiPA-specific logic.

#### Step 7 — `data_quality/tools/anomaly_tools.py`
- `detect_numerical_outliers`: run IQR + Z-score on ALL numeric columns (>90% float), not just `spesa`
- `detect_categorical_anomalies`: run rare-value detection on ALL string cols with ≤50 unique values

#### Step 8 — `data_quality/tools/remediation_tools.py`
- `calculate_reliability_score`: add `col_scale = min(10 / max(column_count, 1), 1.0)` to prevent over-penalizing wide datasets

#### Step 9 — `main.py`
Replace hardcoded path with argparse:
```bash
python main.py                                 # defaults to data/spesa.csv
python main.py data/attivazioniCessazioni.csv
python main.py /absolute/path/to/any.csv
```

### Key Design Decisions
- **Profile in messages, not TeamState**: avoids modifying all 5 team files; profile JSON embedded in initial human message per team
- **No LLM in fix functions**: fixes stay deterministic Python
- **YYYYMM won't be mis-parsed as dates**: profiler classifies 6-digit integers as `numeric`, not `date`
- **`data_quality/teams/*.py` files unchanged**

### Files to Modify (10 total, 1 new)

| File | Change |
|------|--------|
| `data_quality/state.py` | Add `dataset_profile: dict` |
| `data_quality/tools/profiler.py` | **CREATE NEW** |
| `data_quality/graph.py` | Add profiler node, pass profile everywhere |
| `data_quality/tools/schema_tools.py` | Rewrite tools + fix fn |
| `data_quality/tools/completeness_tools.py` | Update fix fn signature |
| `data_quality/tools/consistency_tools.py` | Full rewrite |
| `data_quality/tools/anomaly_tools.py` | Rewrite both tools |
| `data_quality/tools/remediation_tools.py` | Minor scoring update |
| `main.py` | CLI arg parsing |
| `CLAUDE.md` | This update |

### Verification
```bash
python main.py                                  # spesa.csv — should produce same outputs
python main.py data/attivazioniCessazioni.csv   # second dataset — must work without code changes
grep -n "rata\|tipo_imposta\|cod_tipoimposta" data_quality/tools/*.py  # should return nothing
```
