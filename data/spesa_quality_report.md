# Data Quality Report

| | |
|---|---|
| **Dataset** | `spesa.csv` |
| **Date** | April 07, 2026 |
| **Teams executed** | 5 |
| **Messages exchanged** | 5 |
| **Reliability score** | **45/100**  —  Grade **C** |

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

> **Reliability Score: 45/100  |  Grade C**

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

Based on the data type validation performed on the dataset `spesa.csv`, I have identified several columns where the actual data format deviates from the expected semantic type.

### Data Type Validation Findings

| Column | Expected Type | Contaminated Rows | Severity | Example Bad Values |
| :--- | :--- | :--- | :--- | :--- |
| **rata** | Numeric | 510 (6.76%) | MODERATE | 'Rata 2024', '2024-06', '2024-04', '2023-12', 'FEB-2024' |
| **spesa** | Numeric | 227 (3.01%) | MODERATE | 'N.D.', '€25667.3', '€1002.17', '€3676.04', '€6583887536.34' |
| **aggregation-time** | Date | 602 (7.98%) | MODERATE | '2024/04/11', '24.10.2024', '11.06.2024', '11-05-24', '11/07/2024' |
| **SPESA TOTALE** | Numeric | 227 (3.01%) | MODERATE | 'N.D.', '€25667.3', '€1002.17', '€3676.04', '€6583887536.34' |

---

### Detailed Analysis

*   **rata (Numeric)**
    *   **Root Cause:** Inconsistent formatting. The column contains a mix of pure numeric strings (e.g., "202402"), descriptive text ("Rata 2024"), and various date-like separators (hyphens).
*   **spesa & SPESA TOTALE (Numeric)**
    *   **Root Cause:** Presence of currency symbols ("€") and placeholder strings ("N.D."). These characters prevent the columns from being parsed as standard numeric types.
*   **aggregation-time (Date)**
    *   **Root Cause:** Format mixing. While the column is mostly ISO-like, it contains a variety of conflicting date formats including different separators (slashes, dots, hyphens) and varying day/month/year orderings (e.g., "24.10.2024" vs "11-05-24").

*Note: Only columns with identified type violations are listed above. Columns not mentioned were found to be consistent with their expected semantic types.*

---

### Section 1 — Naming Violations

The following columns violate the standard `snake_case` naming convention.

| Column | Violation(s) | Recommended Name |
| :--- | :--- | :--- |
| `2cod_imposta` | Starts with a digit | `cod_imposta_v2` |
| `SPESA TOTALE` | Contains uppercase, contains space | `spesa_totale` |
| `Tipo Imposta` | Contains uppercase, contains space | `tipo_imposta_v2` |
| `aggregation-time` | Contains hyphen | `aggregation_time` |
| `cod imposta ext` | Contains space | `cod_imposta_ext` |
| `ente%code` | Contains special character (`%`) | `ente_code` |

---

### Section 2 — Duplicate / Redundant Columns

The following columns appear to be redundant based on name similarity and high value overlap.

| Redundant Column | Likely Original | Value Overlap | Action |
| :--- | :--- | :--- | :--- |
| `cod imposta ext` | `cod_imposta` | 85% | Drop `cod imposta ext` |
| `ente%code` | `ente` | 96% | Drop `ente%code` |
| `SPESA TOTALE` | `spesa` | 99% | Drop `SPESA TOTALE` |

*Note: The high value overlap suggests these columns contain identical or near-identical data. It is recommended to retain the original, cleaner-named column and drop the redundant versions.*

---

### Section 3 — Impact Summary

*   **Total Naming Violations:** 6
*   **Total Duplicate Pairs Found:** 3
*   **Overall Naming Quality:** **Poor.** The dataset contains multiple columns with non-standard characters, spaces, and casing inconsistencies. Furthermore, the presence of redundant columns suggests potential data lineage issues or improper merging during the data preparation phase. Cleaning these names and removing duplicates is highly recommended to improve data usability and prevent downstream errors.

---

## 🔍 Completeness Analysis

> *Missing values, null rates, and sparse column detection.*

### Missing Value and Placeholder Detection Report

**Dataset Overview:**
*   **Total Rows:** 7,543
*   **Rows with at least one missing value:** 7,543 (100% of the dataset)

---

### Detailed Findings

| Column | Semantic Type | Null Count | Null % | Placeholder Count | Effective Missing % | Severity |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `_id` | identifier | 7,543 | 100% | 0 | 100% | CRITICAL |
| `fonte_dato` | categorical | 7,468 | 99.01% | 0 | 99.01% | CRITICAL |
| `note` | categorical | 7,393 | 98.01% | 0 | 98.01% | CRITICAL |
| `area_geografica` | categorical | 1,582 | 20.97% | 0 | 20.97% | HIGH |
| `rata` | numeric | 510 | 6.76% | 0 | 6.76% | MODERATE |
| `descrizione` | categorical | 456 | 6.05% | 0 | 6.05% | MODERATE |
| `imposta` | categorical | 373 | 4.94% | 0 | 4.94% | LOW |
| `ente` | numeric | 302 | 4.00% | 0 | 4.00% | LOW |
| `cod_imposta` | numeric | 225 | 2.98% | 2.98% | 2.98% | LOW |
| `spesa` | numeric | 112 | 1.48% | 0 | 1.48% | LOW |

---

### Analysis Notes:
*   **`_id`**: This column is entirely empty (100% null), rendering it useless as an identifier.
*   **`fonte_dato` & `note`**: These columns exhibit extreme sparsity (>98%), suggesting they are either largely unpopulated or contain metadata that is rarely recorded.
*   **`area_geografica`**: With ~21% missing, this column falls into the HIGH severity category and may require imputation strategies if used for regional analysis.
*   **Placeholder Detection**: The current analysis focused on null/NaN values. No explicit placeholder strings (e.g., "N/A", "Unknown") were identified as distinct from the null values in this pass.
*   **Data Integrity**: Every single row in the dataset contains at least one missing value, indicating a significant data quality challenge regarding record completeness.

---

### Section 1 — Overall Dataset Completeness
**Overall Completeness Rate: 73.52%**
**Classification: MODERATE**

---

### Section 2 — Per-Column Completeness

| Column | Completeness % | Missing Rows |
| :--- | :--- | :--- |
| `_id` | 0.00% | 7,543 |
| `fonte_dato` | 0.99% | 7,468 |
| `note` | 1.99% | 7,393 |
| `area_geografica` | 79.03% | 1,582 |
| `rata` | 93.24% | 510 |
| `descrizione` | 93.95% | 456 |
| `imposta` | 95.06% | 373 |
| `ente` | 96.00% | 302 |
| `cod_imposta` | 97.02% | 225 |
| `spesa` | 98.52% | 112 |
| `aggregation_time` | 100.00% | 0 |
| `cod_tipoimposta` | 100.00% | 0 |
| `tipo_imposta` | 100.00% | 0 |

*   **<5% (Critical):** `_id`, `fonte_dato`, `note`
*   **5–50%:** None
*   **50–95%:** `area_geografica`, `rata`, `descrizione`
*   **≥95%:** `imposta`, `ente`, `cod_imposta`, `spesa`, `aggregation_time`, `cod_tipoimposta`, `tipo_imposta`

---

### Section 3 — Analysis Implications

For columns with completeness below 80%, the following downstream impacts are expected:

*   **`_id` (0%):** This column is entirely empty. Any analysis relying on unique record identification or primary key joins will fail completely.
*   **`fonte_dato` (0.99%) & `note` (1.99%):** These columns are effectively unusable for any statistical analysis or machine learning feature engineering due to extreme sparsity. They should likely be excluded from models.
*   **`area_geografica` (79.03%):** With ~21% of data missing, any geographic segmentation or regional aggregation will be significantly skewed. If the missingness is not random (e.g., specific regions are under-reported), this will introduce substantial bias into regional performance metrics or predictive models.

---

### Sparsity Report: `spesa_v1.csv`

Following the analysis of the dataset, I have identified columns that exhibit high levels of missingness. Below is the diagnostic report and recommendations for your data pipeline.

---

### Section 1 — Removal Candidates (>95% missing)
These columns are effectively empty and provide negligible analytical value. They consume memory and can introduce noise or errors into machine learning models.

*   **`note`** (98.01% missing): Categorical. **Recommendation: DROP.** This column is almost entirely empty. Unless the remaining 1.99% of data is critical for a specific audit, it should be removed.
*   **`fonte_dato`** (99.01% missing): Categorical. **Recommendation: DROP.** With only 1% of rows populated, this column cannot provide meaningful statistical insights or feature importance.

*(Note: While `_id` was flagged as 100% missing in the automated check, based on your provided profile, it appears to be a populated identifier. Please verify if the file path or the schema provided in the prompt has a mismatch.)*

---

### Section 2 — High-Sparsity Columns (50–95% missing)
*No columns were found in the 50–95% range.*

The column **`area_geografica`** is the next most sparse, with **20.97%** missing. While this falls below the 50% threshold, it is significant enough to warrant attention:
*   **Recommendation:** Since this is a categorical variable, I recommend **imputation** (e.g., assigning a "Missing/Unknown" category) rather than dropping, as geographic context is often vital for financial or administrative datasets.

---

### Section 3 — Sparsity Impact Assessment

*   **Remaining Columns:** If the two columns identified in Section 1 are dropped, the dataset will be reduced from 18 to 16 columns.
*   **Systematic Collection Failure:** The high sparsity in `note` and `fonte_dato` suggests a systematic collection failure—likely these fields were not consistently implemented in the source system or were only added for specific, rare edge cases.
*   **Data Quality Note:** You have several columns that appear to be redundant or duplicates (e.g., `cod_imposta`, `2cod_imposta`, and `cod imposta ext`). While these are not "sparse," they represent a high risk for data inconsistency. I recommend reviewing these for potential consolidation during your next cleaning phase.

**Next Steps:**
1.  Verify the status of the `_id` column to ensure the data ingestion process is reading the file correctly.
2.  Proceed with dropping `note` and `fonte_dato`.
3.  Implement an imputation strategy for `area_geografica` to preserve the 79% of valid geographic data.

---

## ✅ Consistency Validation

> *Format normalisation, cross-column logic, and duplicate rows.*

### Data Quality Findings Report: `spesa_v2.csv`

Following the consistency validation of the provided dataset, the following format and case inconsistencies were identified.

---

#### Section 1 — Date/Period Format Inconsistencies

The column `aggregation-time` contains significant date format fragmentation. While the majority of entries follow the ISO-8601 standard, a substantial portion of the data is stored in various legacy or regional formats.

*   **Column:** `aggregation-time`
*   **Format Distribution:**
    *   **ISO-8601 datetime:** 6,941 occurrences
    *   **DD/MM/YYYY:** 134 occurrences
    *   **DD-MM-YY:** 131 occurrences
    *   **YYYY/MM/DD:** 117 occurrences
    *   **DD.MM.YYYY:** 111 occurrences
    *   **Other/Unrecognized:** 109 occurrences
*   **Recommendation:** Standardize all entries to **ISO-8601 (YYYY-MM-DD)**.
*   **Minority Format Examples:**
    *   `11/03/2024` (DD/MM/YYYY)
    *   `11-03-24` (DD-MM-YY)
    *   `2024/03/11` (YYYY/MM/DD)
    *   `11.03.2024` (DD.MM.YYYY)

---

#### Section 2 — Case and Whitespace Inconsistencies

The column `tipo_imposta` exhibits case sensitivity issues. This is a common source of data fragmentation in categorical analysis.

*   **Column:** `tipo_imposta`
*   **Detected Variants:**
    *   `Erariali`
    *   `erariali`
    *   `ERARIALI`
*   **Downstream Impact:** Any `groupby`, `pivot_table`, or filtering operation performed on this column will treat these as three distinct categories rather than one. This will result in fragmented counts, inaccurate reporting, and broken business logic in downstream machine learning models or dashboards.

---

#### Section 3 — Severity and Priority

| Issue | Column | Severity | Priority |
| :--- | :--- | :--- | :--- |
| Mixed Date Formats | `aggregation-time` | **CRITICAL** | **HIGH** |
| Case Inconsistency | `tipo_imposta` | **HIGH** | **HIGH** |

**Rationale:**
*   **CRITICAL (Date Formats):** Time-series analysis is impossible or highly inaccurate when dates are parsed inconsistently. This will likely cause errors in any temporal aggregation.
*   **HIGH (Case Inconsistency):** This directly impacts the integrity of categorical features, leading to "hidden" data fragmentation that is often overlooked until aggregate totals fail to match expected values.

---

### Cross-Column Logic Findings Report

The validation of `spesa_v2.csv` has identified significant logical inconsistencies. Below is the detailed breakdown of the findings.

---

#### Section 1 — Code→Label Mapping Violations
The analysis detected several columns acting as codes that fail to maintain a 1-to-1 relationship with their corresponding labels. In all cases, 100% of the code values were found to map to multiple labels, indicating a severe data integrity issue.

*   **`cod_imposta` ↔ `imposta` / `tipo_imposta` / `area_geografica`**:
    *   **Violation**: A single `cod_imposta` (e.g., "10") maps to multiple `tipo_imposta` values (e.g., "Varie" and "Da definire").
    *   **Impact**: This prevents reliable grouping or filtering by tax type. Any aggregation performed using these codes will result in corrupted or misleading totals.
*   **`cod_tipoimposta` ↔ `tipo_imposta`**:
    *   **Violation**: `cod_tipoimposta` "4" maps to both "Varie" and "Da definire".
    *   **Impact**: Similar to the above, this indicates that the classification schema is not strictly enforced, rendering the code column unreliable for categorical analysis.
*   **`rata` (Period Code) ↔ `cod_tipoimposta` (Month)**:
    *   **Violation**: There is a 92.1% mismatch between the month extracted from the `rata` column (YYYYMM) and the `cod_tipoimposta` column.
    *   **Impact**: This suggests that `cod_tipoimposta` is either mislabeled or contains data that does not represent a calendar month, despite the semantic expectation.

**Recommendation**: These mappings must be audited against the source system's master data. The high percentage of inconsistencies suggests a systemic issue in the data extraction or transformation pipeline.

---

#### Section 2 — Negative Values in Non-Negative Columns
The column `spesa` (representing expenditure) contains negative values, which is logically inconsistent with the semantic definition of "expenditure" or "total spend."

*   **Column**: `spesa`
*   **Count of Negative Values**: 11
*   **Minimum Value**: -999,999.50
*   **Example Values**: -857.68, -999,999.50, -1,755.33, -2,298.46, -1.00

**Discussion**:
While negative values in financial datasets sometimes represent "reversals" or "corrections," their presence in a primary expenditure column without a corresponding "transaction type" flag is dangerous. If these are not intentional reversals, they are likely data entry errors or encoding bugs.

**Recommendation**: Investigate the source of these 11 records. If they are corrections, they should be explicitly marked in a separate column (e.g., `is_reversal`) rather than simply appearing as negative amounts in the `spesa` column.

---

#### Section 3 — Summary
The dataset exhibits critical logical failures. The lack of a stable 1-to-1 mapping between codes and labels, combined with unexplained negative expenditure values, suggests that the dataset is currently unsuitable for automated financial reporting or analytical modeling without significant data cleaning and reconciliation.

---

### Duplication Report: `spesa_v2.csv`

This report details the findings of the duplicate row detection analysis performed on the provided dataset.

---

#### Section 1 — Exact Duplicates
*   **Count:** 41 rows
*   **Percentage:** ~0.54% of the total dataset (7,543 rows).
*   **Impact:** These rows are identical across all 18 columns. In financial datasets, exact duplicates lead to direct inflation of totals (e.g., double-counting expenditures), which can significantly skew budget reports and analytical models.
*   **Examples:**
    1.  `MINISTERO DELL'ISTRUZIONE E DEL MERITO` | `rata: 202408` | `spesa: 10162467.03`
    2.  `AUTORITA' DI BACINO DISTRETTUALE DEL FIUME PO` | `rata: 202406` | `spesa: 16365.44`
    3.  `Unknown` (Ente 974) | `rata: 202409` | `spesa: 342712.36`

---

#### Section 2 — Near-Duplicates
*   **Key Columns Selected:** `spesa`
*   **Reasoning:** The `spesa` column was selected as the key because it exhibits moderate cardinality (86.08%) and represents the primary quantitative value of the record.
*   **Affected Rows:** 1,509 rows share a `spesa` value with at least one other row.
*   **Examples:**
    1.  `spesa: 256.53` (Found in multiple records with different `aggregation-time` or `ente` values).
    2.  `spesa: 129.79` (Found in multiple records).
    3.  `spesa: 42.36` (Found in multiple records).
*   **Interpretation:**
    *   **(b) Legitimate updates/entities:** It is highly probable that these are not duplicates but rather different entities or different time periods that happen to have the exact same expenditure amount.
    *   **Recommendation:** These should **not** be removed automatically. They require domain review to determine if the `spesa` value is unique to a specific transaction or if it represents a recurring fixed cost across different entities.

---

#### Section 3 — Deduplication Recommendation
*   **Exact Duplicates:** These are safe to drop. Removing these 41 rows will improve data integrity without losing information.
*   **Near-Duplicates:** These require domain review. Do not remove these rows, as they likely represent distinct financial transactions that share identical monetary values.
*   **Estimated Remaining Rows:** After removing the 41 exact duplicates, the dataset will contain **7,502 rows**.

---

## ⚠️ Anomaly Detection

> *Numerical outliers (IQR / Z-score) and rare categorical values.*

### Numerical Outlier Detection Report

This report details the statistical anomalies identified in the `spesa_v3.csv` dataset. The analysis focused on numeric columns using both Interquartile Range (IQR) and Z-score methods.

---

#### 1. Column: `spesa`
*   **Distribution Summary:** Mean: 25,208,369.14 ± 316,700,992.80; Min: -999,999.50; Max: 12,300,103,933.74
*   **IQR Outliers:** 1,352 (Lower Fence: -911,507.07, Upper Fence: 1,526,966.62)
*   **Z-score Outliers:** 39
*   **Top 5 Extreme Values:** 12,300,103,933.74, 8,101,180,654.38, 7,215,139,997.19, 6,583,887,536.34, 6,526,086,642.99
*   **Root Cause Assessment:** The extreme values (billions) are significantly higher than the mean, suggesting either genuine large-scale government expenditures or potential unit errors (e.g., values recorded in cents instead of euros). The presence of negative values (-999,999.50) is highly suspicious for a "spesa" (expenditure) column and likely indicates data entry errors or accounting adjustments.
*   **Recommended Action:** Flag for review. Investigate the negative values immediately and verify the unit of measurement for the extreme positive values.
*   **Severity:** **HIGH** (18% of rows are IQR outliers).

#### 2. Column: `rata`
*   **Distribution Summary:** Mean: 202,398.14 ± 25.40; Min: 202,312; Max: 202,410
*   **IQR Outliers:** 594 (Lower Fence: 202,395.5, Upper Fence: 202,415.5)
*   **Z-score Outliers:** 594
*   **Root Cause Assessment:** The column represents a date/period format (YYYYMM). The outliers are simply values from the year 2023 (e.g., 202312), while the majority of the data is from 2024. These are not errors but temporal variations.
*   **Recommended Action:** Investigate-only. No action required as these are valid temporal data points.
*   **Severity:** **MODERATE** (7.9% of rows).

#### 3. Column: `ente`
*   **Distribution Summary:** Mean: 679.56 ± 1,059.17; Min: 1; Max: 8,017
*   **IQR Outliers:** 206 (Lower Fence: -1,386, Upper Fence: 2,358)
*   **Z-score Outliers:** 206
*   **Root Cause Assessment:** These represent entity codes. The outliers are simply higher-numbered codes.
*   **Recommended Action:** Investigate-only. These are likely valid identifiers.
*   **Severity:** **MODERATE** (2.7% of rows).

---

### Summary Table

| Column | IQR Outlier Count | Z-score Outlier Count | Max Value | Severity | Likely Cause |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `spesa` | 1,352 | 39 | 12,300,103,933.74 | HIGH | Unit error or data entry error |
| `rata` | 594 | 594 | 202,410 | MODERATE | Temporal variation (2023 vs 2024) |
| `ente` | 206 | 206 | 8,017 | MODERATE | Valid identifier variation |

**Note:** The `SPESA TOTALE` column was excluded from the tool's automated numeric analysis as it mirrors the `spesa` column; findings for `spesa` apply to both.

---

### Categorical Anomaly Detection Report

**Dataset:** `C:\Users\sebas\Documents\GitHub\ML_MiSoMa_reply2026\data\spesa_v3.csv`

---

#### Section 1 — Rare Values
Rare values are defined as those appearing in <0.5% of total rows.

| Column | Rare Value | Count | % of Total | Classification | Recommendation |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **imposta** | `DA VERIFICARE` | 4 | 0.05% | Probable placeholder/error | Investigate/Convert to NaN |
| **imposta** | `imposta x` | 3 | 0.04% | Probable placeholder/error | Investigate/Convert to NaN |
| **imposta** | `TBD` | 2 | 0.03% | Probable placeholder/error | Investigate/Convert to NaN |
| **imposta** | `Altro` | 1 | 0.01% | Valid-but-infrequent | Keep as-is |
| **tipo_imposta** | `Da definire` | 2 | 0.03% | Probable placeholder/error | Convert to NaN |
| **tipo_imposta** | `Mista` | 2 | 0.03% | Valid-but-infrequent | Keep as-is |

---

#### Section 2 — Remaining Invalid Markers
The following columns contain string-based invalid markers that were not cleaned during the initial pipeline phase.

| Column | Invalid Marker | Count | % of Total |
| :--- | :--- | :--- | :--- |
| **area_geografica** | `Unknown` | 1574 | 20.87% |
| **imposta** | `Unknown` | 373 | 4.94% |

*Note: These columns require immediate re-cleaning to convert these markers to proper `NaN` values.*

---

#### Section 3 — Value Distribution Health

*   **`area_geografica`**: Contains 4 unique values. The high frequency of `Unknown` (20.87%) suggests a significant data quality issue regarding geographic attribution.
*   **`imposta`**: Contains 16 unique values. The distribution is fragmented by both invalid markers (`Unknown`) and placeholder strings (`DA VERIFICARE`, `TBD`), which may interfere with categorical encoding.
*   **`tipo_imposta`**: Contains 7 unique values. The distribution appears relatively healthy, though the presence of `Da definire` should be addressed.

---

#### Section 4 — Impact Summary

*   **Total columns with anomalies:** 3
*   **Total anomalous values identified:** 1959
*   **Column with worst anomaly rate:** `area_geografica` (20.87% invalid markers).

**Conclusion:** The dataset requires a targeted cleaning pass to address the high volume of `Unknown` markers in `area_geografica` and `imposta`, as well as the removal of placeholder strings in the `imposta` column before proceeding to feature engineering or model training.

---

## 🛠️ Remediation & Reliability

> *Correction suggestions and final reliability score.*

Based on the comprehensive analysis of the dataset `spesa.csv` across the four data quality dimensions (Schema, Completeness, Consistency, and Anomaly), I have synthesized the following remediation plan.

### Remediation Report

#### PRIORITY 1 — REMOVE
*   **Drop Columns:** `note` and `fonte_dato` (due to >98% missingness).
*   **Remove Rows:** Delete the 41 confirmed exact-duplicate rows to prevent inflation of financial totals.

#### PRIORITY 2 — RENAME / RESTRUCTURE
*   **Standardize Names:** Ensure all column names follow `snake_case` (e.g., `2cod_imposta` → `cod_imposta_v2`, `Tipo Imposta` → `tipo_imposta_v2`, `ente%code` → `ente_code`).

#### PRIORITY 3 — TYPE COERCION
*   **Numeric Coercion:** Ensure `spesa` is strictly float.
*   **Date Normalization:** Finalize the standardization of `aggregation_time` to ISO-8601 `YYYY-MM-DD`.

#### PRIORITY 4 — IMPUTE / FILL
*   **Categorical Cleaning:** Convert placeholder strings (`DA VERIFICARE`, `TBD`, `Da definire`, `Unknown`) in `imposta`, `tipo_imposta`, and `area_geografica` to `NaN` to allow for consistent imputation or exclusion.
*   **Imputation:** Fill remaining `NaN` values in categorical columns with the mode or a dedicated 'Unknown' category, and numeric columns with the median.

#### PRIORITY 5 — STANDARDISE
*   **Categorical Normalization:** Unify case variants in `tipo_imposta` (e.g., `Erariali`, `erariali`, `ERARIALI` → `erariali`).

#### PRIORITY 6 — INVESTIGATE
*   **Financial Logic:** Investigate the 11 negative values in `spesa`. Determine if these are legitimate reversals or data entry errors.
*   **Outlier Review:** Audit the extreme positive values in `spesa` (billions) to confirm if they represent genuine large-scale expenditures or a unit-of-measurement error (e.g., cents vs. euros).
*   **Mapping Audit:** Reconcile the 1-to-1 mapping between `cod_imposta` and `tipo_imposta` with the source system, as current data shows significant logical corruption.

---

### Summary Table

| Priority | Column(s) | Issue | Action | Expected Outcome |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `note`, `fonte_dato` | >95% Empty | Drop columns | Reduced noise/memory usage |
| 1 | All | Exact Duplicates | Remove 41 rows | Accurate financial totals |
| 2 | Various | Non-snake_case | Rename columns | Improved code readability |
| 3 | `spesa` | Mixed types | Coerce to float | Enable math operations |
| 4 | `area_geografica` | Placeholder strings | Convert to `NaN` | Clean categorical features |
| 5 | `tipo_imposta` | Case variants | Normalize to lowercase | Unified category counts |
| 6 | `spesa` | Negative values | Audit/Flag | Data integrity verification |
| 6 | `spesa` | Extreme outliers | Unit verification | Corrected scale/magnitude |

**Reliability Score:** **45/100**
*The dataset is currently unreliable for automated financial reporting due to critical logical mapping failures, unexplained negative expenditure values, and significant categorical fragmentation. Remediation of the identified logical and outlier issues is mandatory before the data can be considered trustworthy.*

---

### Section 1 — Final Score and Grade
**Reliability Score: 68/100 — Grade: C**

The dataset is currently in a "C" state, indicating significant quality issues that hinder reliable analysis. While the data is structurally present, the high frequency of missing values, logical inconsistencies in code-to-label mappings, and the presence of extreme outliers in the `spesa` column make it unsuitable for production-grade financial reporting without further intervention.

---

### Section 2 — Deduction Breakdown

| Category | Issue | Deduction | Running Score |
| :--- | :--- | :--- | :--- |
| **Baseline** | Starting Score | 0 | 100 |
| **Completeness** | 73.52% Global Completeness | -13.24 | 86.76 |
| **Schema** | 4 Type Violations (scaled) | -2.00 | 84.76 |
| **Naming** | 6 Naming Violations (scaled) | -0.33 | 84.43 |
| **Consistency** | 1 Format, 3 Cross-col, 41 Duplicates | -14.41 | 70.02 |
| **Anomaly** | IQR Outliers in 3 columns | -2.00 | 68.02 |

*(Note: Deductions are scaled based on the 18-column dataset width.)*

---

### Section 3 — Score Improvement Roadmap

1.  **Resolve Logical Inconsistencies (Priority: High):** Audit the `cod_imposta` and `cod_tipoimposta` mappings. The current 1-to-many relationship between codes and labels is the single largest threat to data integrity. **(Est. +10 pts)**
2.  **Standardize `spesa` and Outliers (Priority: High):** Investigate the 11 negative values and the extreme billion-euro outliers. If these are unit errors (e.g., cents vs. euros), correcting them will stabilize the distribution. **(Est. +8 pts)**
3.  **Clean Categorical Placeholders (Priority: Medium):** Convert `Unknown`, `DA VERIFICARE`, and `TBD` markers in `area_geografica` and `imposta` to true `NaN` values to allow for proper imputation or exclusion. **(Est. +6 pts)**

---

### Section 4 — Expected Post-Fix Score
After implementing the recommended corrections (addressing logical mappings, cleaning placeholders, and resolving the `spesa` unit errors), the estimated reliability score is **88/100 (Grade: B)**.

Score: 68/100  Grade: C

---
