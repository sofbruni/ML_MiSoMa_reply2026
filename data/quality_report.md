# Data Quality Report

| | |
|---|---|
| **Dataset** | `spesa.csv` |
| **Date** | March 31, 2026 |
| **Teams executed** | 5 |
| **Messages exchanged** | 5 |

---

## Contents

1. [Executive Summary](#executive-summary)
2. [🗂️ Schema Validation](#schema-validation)
3. [🔍 Completeness Analysis](#completeness-analysis)
4. [✅ Consistency Validation](#consistency-validation)
5. [⚠️ Anomaly Detection](#anomaly-detection)
6. [🛠️ Remediation & Reliability](#remediation-reliability)

---

## Executive Summary

The pipeline ran **5 specialist teams** in sequence. Each team analysed the
working CSV and applied targeted fixes before passing control to the next team.
The table below summarises what each team was responsible for.

| Team | Responsibility | Status |
|------|---------------|--------|
| 🗂️ **Schema Validation** | Data types, column naming conventions, and duplicate columns. | ✅ Completed |
| 🔍 **Completeness Analysis** | Missing values, null rates, and sparse column detection. | ✅ Completed |
| ✅ **Consistency Validation** | Format normalisation, cross-column logic, and duplicate rows. | ✅ Completed |
| ⚠️ **Anomaly Detection** | Numerical outliers (IQR / Z-score) and rare categorical values. | ✅ Completed |
| 🛠️ **Remediation & Reliability** | Correction suggestions and final reliability score. | ✅ Completed |

---

## 🗂️ Schema Validation

> *Data types, column naming conventions, and duplicate columns.*

The naming convention check for `spesa.csv` has been completed. Several issues regarding column naming and duplicate columns were identified.

### **Naming Convention Findings**

#### **1. Naming Violations**
The following columns violate standard naming conventions (e.g., snake_case, no special characters, no leading digits):

*   **`2cod_imposta`**: Starts with a digit.
*   **`SPESA TOTALE`**: Contains uppercase letters and a space.
*   **`Tipo Imposta`**: Contains uppercase letters and a space.
*   **`aggregation-time`**: Contains a hyphen (should use an underscore).
*   **`cod imposta ext`**: Contains spaces.
*   **`ente%code`**: Contains a special character (`%`).

#### **2. Duplicate Columns**
The following columns are duplicates of existing columns and should be removed to avoid redundancy:

*   **`2cod_imposta`**: Duplicate of `cod_imposta`.
*   **`SPESA TOTALE`**: Duplicate of `spesa`.
*   **`Tipo Imposta`**: Duplicate of `tipo_imposta`.
*   **`cod imposta ext`**: Duplicate of `cod_imposta`.
*   **`ente%code`**: Duplicate of `ente`.

---
**Recommendation:** 
Before proceeding with data cleaning, it is recommended to drop the identified duplicate columns and rename the remaining columns to follow a consistent `snake_case` format (e.g., `tipo_imposta`, `spesa_totale`, `aggregation_time`).

---

## 🔍 Completeness Analysis

> *Missing values, null rates, and sparse column detection.*

The completeness analysis for the dataset `spesa_v1.csv` is complete. Out of **7,543 total rows**, **7,542 rows** contain at least one missing value.

Below is the breakdown of missing values per column:

### Completeness Report

| Column | Missing Count | Missing Percentage |
| :--- | :--- | :--- |
| **fonte_dato** | 7,468 | 99.01% |
| **note** | 7,393 | 98.01% |
| **area_geografica** | 1,582 | 20.97% |
| **descrizione** | 456 | 6.05% |
| **imposta** | 373 | 4.94% |
| **ente** | 302 | 4.00% |
| **cod_imposta** | 225 | 2.98% |
| **spesa** | 112 | 1.48% |

### Key Findings:
*   **Highly Sparse Columns:** The columns `fonte_dato` and `note` are almost entirely empty (99% and 98% missing, respectively). These columns may not be suitable for analysis or machine learning tasks without significant imputation or removal.
*   **Moderate Sparsity:** `area_geografica` has a notable amount of missing data (~21%), which should be investigated to determine if the missingness is random or systematic.
*   **High Completeness:** The `spesa` column is the most complete, with only 1.48% missing values.

---

## ✅ Consistency Validation

> *Format normalisation, cross-column logic, and duplicate rows.*

The duplicate detection process for `spesa_v2.csv` has revealed significant data quality issues that, when combined with the previously identified formatting and logical inconsistencies, suggest the dataset requires a robust cleaning pipeline.

### Duplicate Findings
*   **Exact Duplicates:** 41 rows are identical across all columns. These should be removed to prevent artificial inflation of expenditure totals.
*   **Near-Duplicates (Key Columns):** 806 rows share the same `rata`, `ente`, and `cod_imposta` but have different values in other fields (e.g., `spesa` or `aggregation_time`). This suggests either:
    *   Multiple entries for the same tax/entity in the same period that should have been aggregated.
    *   Data entry errors where the same transaction was recorded multiple times with slight variations.

### Summary of Data Quality Status
| Issue Type | Severity | Impact |
| :--- | :--- | :--- |
| **Formatting** | High | Prevents chronological analysis and grouping. |
| **Logical Contradictions** | Critical | Invalidates the relationship between codes and categories. |
| **Duplicates** | Medium | Skews statistical analysis and total expenditure calculations. |
| **Negative Values** | Medium | Potential accounting errors or unhandled refund logic. |

---

### Recommendation
Given the complexity of these issues—specifically the 806 near-duplicates and the 383 logical mismatches—**I strongly recommend proceeding with a Python-based cleaning script.** 

This script will:
1.  **Deduplicate:** Remove exact duplicates and provide a strategy for handling the 806 near-duplicates (e.g., aggregating `spesa` by the key columns).
2.  **Standardize:** Apply the `YYYY-MM` format for `rata` and ISO-8601 for `aggregation_time`.
3.  **Reconcile:** Force the `tipo_imposta` to align with the `cod_tipoimposta` based on a master mapping.
4.  **Validate:** Flag or transform negative `spesa` values.

**Would you like me to generate this Python script now?** If so, please confirm if you would like the near-duplicates to be **aggregated** (summing the `spesa` values) or **filtered** (keeping only the most recent entry based on `aggregation_time`).

---

## ⚠️ Anomaly Detection

> *Numerical outliers (IQR / Z-score) and rare categorical values.*

Following the numerical outlier analysis, I have performed a categorical anomaly detection on the dataset. Below are the findings regarding rare and potentially problematic categorical values.

### Categorical Anomaly Detection Report

The analysis identified several categories that appear with a frequency of less than 0.5% of the total observations, as well as potential data quality issues (e.g., "Unknown" markers).

#### 1. `imposta` (Tax Type)
*   **Rare Values:** `DA VERIFICARE` (4), `imposta x` (3), `TBD` (2), `Altro` (1).
*   **Data Quality:** 373 entries are marked as `Unknown`.
*   **Observation:** The presence of placeholders like "TBD" and "DA VERIFICARE" suggests incomplete data entry that should be cleaned or imputed before modeling.

#### 2. `tipo_imposta` (Tax Category)
*   **Rare Values:** `Mista` (2), `erariali` (1), `ERARIALI` (1).
*   **Observation:** There is a clear case-sensitivity issue (`erariali` vs `ERARIALI`), which should be normalized to a single category.

#### 3. `area_geografica` (Geographic Area)
*   **Data Quality:** 1,574 entries are marked as `Unknown`.
*   **Observation:** This represents a significant portion of the dataset (~21%), which may introduce bias if the missingness is not random.

#### 4. `descrizione` (Description)
*   **Rare Values:** A long tail of 84 unique categories, including specific entities like `MINISTERO DELLA CULTURA` and various `AZIENDA SANITARIA LOCALE` entries.
*   **Data Quality:** 451 entries are marked as `Unknown`.

---

### Summary & Recommendations

1.  **Data Normalization:** Standardize the `tipo_imposta` column to resolve case-sensitivity inconsistencies (e.g., convert all to uppercase).
2.  **Handling "Unknowns":** The high volume of `Unknown` values in `area_geografica` (1,574) and `descrizione` (451) requires a decision: either drop these rows, impute them based on other features, or treat "Unknown" as a distinct category if it carries informational value.
3.  **Cleaning Placeholders:** Remove or reclassify entries containing `TBD`, `DA VERIFICARE`, or `imposta x` in the `imposta` column, as these are likely non-informative or erroneous.
4.  **Feature Engineering:** Given the high number of unique values in `descrizione`, consider grouping these into broader categories (e.g., "Ministry", "Health Authority", "Other") to reduce dimensionality and improve model performance.

---

## 🛠️ Remediation & Reliability

> *Correction suggestions and final reliability score.*

Based on the comprehensive findings from the Schema, Completeness, Consistency, and Anomaly teams, here are the actionable correction suggestions and the overall reliability assessment for the dataset.

### **Correction Suggestions**

#### **1. Data Deduplication & Aggregation**
*   **Action:** Aggregate the 806 near-duplicate rows identified by the Consistency Team.
*   **Method:** Group by `rata`, `ente`, and `cod_imposta`, and sum the `spesa` values to ensure accurate financial totals.

#### **2. Categorical Normalization & Cleaning**
*   **Action:** Standardize `tipo_imposta` by converting all entries to uppercase to resolve case-sensitivity issues (e.g., `erariali` vs `ERARIALI`).
*   **Action:** Replace placeholders (`TBD`, `DA VERIFICARE`, `imposta x`) in the `imposta` column with `NaN` or a consistent `Unknown` label to facilitate cleaner imputation or filtering.
*   **Action:** Group the long tail of unique values in `descrizione` into broader, high-level categories (e.g., "Ministry", "Health Authority", "Other") to reduce dimensionality.

#### **3. Handling Missing Data ("Unknowns")**
*   **Action:** Evaluate the 1,574 `Unknown` entries in `area_geografica` and 451 in `descrizione`.
*   **Recommendation:** If these rows cannot be imputed using domain-specific logic, flag them as a distinct category ("Unknown") rather than dropping them, to preserve the sample size for other features.

#### **4. Logical Validation**
*   **Action:** Implement a master mapping check to reconcile `tipo_imposta` with `cod_tipoimposta`.
*   **Action:** Flag or investigate negative `spesa` values to determine if they represent valid refund logic or accounting errors.

---

### **Reliability Score**

**Reliability Score: 5.5 / 10**

**Justification:**
*   **Strengths:** The dataset has undergone significant initial cleaning (schema standardization, removal of ultra-sparse columns, and basic imputation). The `spesa` column is relatively complete (98.5%).
*   **Weaknesses:** The high volume of "Unknown" values in key categorical fields (`area_geografica`, `descrizione`) and the presence of 806 near-duplicates significantly impact the integrity of the data for analytical modeling.
*   **Conclusion:** While the dataset is structurally sound (following the schema fixes), it is currently **unreliable for high-stakes decision-making** until the logical inconsistencies and near-duplicates are resolved via the recommended aggregation and normalization steps.

---
