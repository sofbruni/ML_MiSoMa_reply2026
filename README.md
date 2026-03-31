# NoiPA — Multi-Agent Data Quality System

A multi-agent pipeline for automated data quality assessment of NoiPA (Italian Public Administration payroll) CSV datasets.
Built for the LUISS × Reply university project — 2026.

## Architecture

```
supervisor.py
├── schema_agent.py        Agent 1 — Column naming, dtype, duplicate detection
├── completeness_agent.py  Agent 2 — Null counts, placeholders, sparse columns
├── consistency_agent.py   Agent 3 — Format consistency, cross-column mismatches, duplicates
├── anomaly_agent.py       Agent 4 — IQR outliers, rare categorical values
└── remediation_agent.py   Agent 5 — LLM suggestions + programmatic fixes → cleaned CSV
```

Each agent uses **pandas/numpy/scipy** to compute statistics and **Gemini 2.0 Flash** to interpret findings and generate scores.

## Installation

```bash
cd project_data_quality
pip install -r requirements.txt
```

Ensure the `.env` file exists with:
```
GEMINI_API_KEY=<your key>
```

## Run the Streamlit App

```bash
cd project_data_quality
streamlit run app.py
```

Then open http://localhost:8501 in your browser.

- Use the **Quick Load** buttons in the sidebar for the bundled datasets.
- Or upload any CSV via the file uploader.
- Click **Run Quality Analysis** to execute the pipeline.

## Run the Pipeline from CLI

```bash
cd project_data_quality

# Full pipeline on spesa.csv
python agents/supervisor.py data/spesa.csv

# Full pipeline on attivazioniCessazioni.csv
python agents/supervisor.py data/attivazioniCessazioni.csv
```

Outputs are saved to `outputs/`:
- `quality_report_<stem>.json` — full quality report
- `cleaned_<stem>.csv` — cleaned dataset

## Run Individual Agents

Each agent is independently runnable:

```bash
python agents/schema_agent.py data/spesa.csv
python agents/completeness_agent.py data/spesa.csv
python agents/consistency_agent.py data/spesa.csv
python agents/anomaly_agent.py data/spesa.csv
python agents/remediation_agent.py data/spesa.csv
```

## Output Report Structure

```json
{
  "filename": "spesa.csv",
  "total_rows": 7543,
  "total_columns": 18,
  "reliability_score": 54.2,
  "schema": { "naming_issues": [...], "dtype_issues": [...], "duplicate_columns": [...], "score": 40 },
  "completeness": { "null_counts": {...}, "sparse_columns": [...], "overall_completeness": 88.5, "score": 72 },
  "consistency": { "format_issues": [...], "cross_column_mismatches": [...], "duplicate_rows": 40, "score": 60 },
  "anomalies": { "numeric_outliers": [...], "categorical_anomalies": [...], "score": 80 },
  "remediation": { "suggestions": [...], "reliability_score": 54.2, "cleaned_csv_path": "outputs/cleaned_spesa.csv" }
}
```

## Reliability Score Formula

```
reliability_score = schema_score * 0.20
                  + completeness_score * 0.25
                  + consistency_score * 0.30
                  + anomaly_score * 0.25
```