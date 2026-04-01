"""
Schema Validation tools:
  - validate_data_types  : checks that columns hold expected types
  - check_naming_conventions : flags naming violations and duplicate columns
  - apply_schema_fixes   : (non-tool) applies all schema corrections to the CSV
"""

import json
import re
import pandas as pd
from langchain_core.tools import tool
from data_quality.config import PLACEHOLDER_VALUES


@tool
def validate_data_types(dataset_path: str) -> str:
    """Check each column for data-type violations and return a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    issues: dict[str, list[str]] = {}

    def _flag(col: str, msg: str):
        issues.setdefault(col, []).append(msg)

    # --- rata: must be 6-digit YYYYMM integer ---
    invalid_rata = df[~df["rata"].str.match(r"^\d{6}$", na=False)]["rata"].unique().tolist()
    if invalid_rata:
        _flag("rata", f"Non-YYYYMM values ({len(invalid_rata)} unique): {invalid_rata[:5]}")

    # --- spesa: must be numeric float ---
    non_num_spesa = df[pd.to_numeric(df["spesa"], errors="coerce").isna()]["spesa"].unique().tolist()
    non_num_spesa = [v for v in non_num_spesa if str(v).strip() not in PLACEHOLDER_VALUES]
    if non_num_spesa:
        _flag("spesa", f"Non-numeric values: {non_num_spesa[:5]}")

    # --- ente: should be integer entity code ---
    if "ente" in df.columns:
        bad_ente = df[
            pd.to_numeric(df["ente"], errors="coerce").isna() & df["ente"].notna()
        ]["ente"].unique().tolist()
        bad_ente = [v for v in bad_ente if str(v).strip() not in PLACEHOLDER_VALUES]
        if bad_ente:
            _flag("ente", f"Non-integer values: {bad_ente[:5]}")

    # --- cod_imposta: should be integer ---
    if "cod_imposta" in df.columns:
        bad_cod = df[
            pd.to_numeric(df["cod_imposta"], errors="coerce").isna() & df["cod_imposta"].notna()
        ]["cod_imposta"].unique().tolist()
        bad_cod = [v for v in bad_cod if str(v).strip() not in PLACEHOLDER_VALUES]
        if bad_cod:
            _flag("cod_imposta", f"Non-integer values: {bad_cod[:5]}")

    return json.dumps({
        "check": "data_type_validation",
        "issues": issues,
        "total_rows": len(df),
        "status": "issues_found" if issues else "ok",
    }, indent=2)


@tool
def check_naming_conventions(dataset_path: str) -> str:
    """Check column names for violations (spaces, special chars, digit-start, all-caps)
    and identify duplicate columns. Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, nrows=0)
    cols = df.columns.tolist()
    naming_issues: dict[str, list[str]] = {}
    duplicate_cols: dict[str, str] = {}

    for col in cols:
        problems: list[str] = []
        stripped = col.strip()
        if stripped != stripped.lower():
            problems.append("not snake_case / contains uppercase")
        if " " in stripped:
            problems.append("contains space")
        if stripped and stripped[0].isdigit():
            problems.append("starts with digit")
        if re.search(r"[%@!#]", stripped):
            problems.append(f"contains special character ({re.findall(r'[%@!#]', stripped)})")
        if "-" in stripped and stripped != "_id":
            problems.append("contains hyphen (use underscore)")
        if problems:
            naming_issues[col] = problems

    # Detect known duplicate columns in spesa.csv
    known_dupes = {
        "Tipo Imposta":    "tipo_imposta",
        "SPESA TOTALE":    "spesa",
        "2cod_imposta":    "cod_imposta",
        "cod imposta ext": "cod_imposta",
        "ente%code":       "ente",
    }
    for duped_col, canonical in known_dupes.items():
        if duped_col in cols and canonical in cols:
            duplicate_cols[duped_col] = f"duplicate of '{canonical}' — safe to drop"

    return json.dumps({
        "check": "naming_convention_check",
        "naming_issues": naming_issues,
        "duplicate_columns": duplicate_cols,
        "status": "issues_found" if (naming_issues or duplicate_cols) else "ok",
    }, indent=2)


# ---------------------------------------------------------------------------
# Fix function (called by the schema team node, not by the LLM)
# ---------------------------------------------------------------------------

def apply_schema_fixes(input_path: str, output_path: str) -> dict:
    """
    Applies all schema corrections:
      1. Drops duplicate/redundant columns
      2. Renames columns with hyphens/spaces
      3. Coerces spesa to float (strips currency symbols)
      4. Replaces placeholder strings with NaN in ente and cod_imposta
      5. Coerces ente and cod_imposta to Int64
    Returns a summary dict of what was changed.
    """
    df = pd.read_csv(input_path, dtype=str)
    changes = []

    # 1. Drop duplicate columns
    cols_to_drop = ["Tipo Imposta", "SPESA TOTALE", "2cod_imposta", "cod imposta ext", "ente%code"]
    dropped = [c for c in cols_to_drop if c in df.columns]
    if dropped:
        df = df.drop(columns=dropped)
        changes.append(f"Dropped duplicate columns: {dropped}")

    # 2. Rename aggregation-time → aggregation_time
    rename_map = {}
    if "aggregation-time" in df.columns:
        rename_map["aggregation-time"] = "aggregation_time"
    if rename_map:
        df = df.rename(columns=rename_map)
        changes.append(f"Renamed columns: {rename_map}")

    # 3. Fix spesa: strip currency symbols and convert to float
    if "spesa" in df.columns:
        df["spesa"] = (
            df["spesa"]
            .str.replace("€", "", regex=False)
            .str.replace("EUR", "", regex=False)
            .str.replace("\ufffd", "", regex=False)  # corrupted euro sign
            .str.strip()
        )
        df["spesa"] = pd.to_numeric(df["spesa"], errors="coerce")
        changes.append("Cleaned and coerced 'spesa' to float")

    # 4 & 5. Fix ente and cod_imposta
    for col in ["ente", "cod_imposta"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: pd.NA if str(x).strip() in PLACEHOLDER_VALUES else x
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
            changes.append(f"Cleaned placeholders and coerced '{col}' to numeric")

    df.to_csv(output_path, index=False)
    return {"fixes_applied": changes, "output_path": output_path}
