# `qadmissions_input_features` — Design Summary

An overview of the design decisions, deviations from the QAdmissions paper, and known areas that may need revisiting. 

The paper referenced throughout is Hippisley-Cox & Coupland, *BMJ Open* 2013;3:e003482.

---

## Overview

`qadmissions_input_features` produces one wide row per active, non-deceased adult (18..100) with known sex. Every column matches the input signature of the QAdmissions Cox model registered in Snowflake's model registry. 

Grain: one row per `person_id`. Build flow:

```
dim_person_demographics (base spine)
  + dim_person_conditions          → b_AF / b_CCF / b_anycancer / b_asthmacopd / b_epilepsy / b_renal / b_manicschiz / b_cvd
  + fct_person_diabetes_register   → b_type1 / b_type2
  + int_smoking_status_latest      → smoke_cat
  + 5 × int_<med>_medications_all  → b_anticoagulant / b_antidepressant / b_antipsychotic / b_corticosteroids / b_nsaid
  + fct_person_sus_apc_recent       → hes_admitprior_cat
  + int_bmi_latest                 → bmi_recorded, and bmi (missing values imputed; see "BMI imputation")
  + dim_person_ethnicity           → ethnicity category, used only to group the BMI imputation
  + int_haemoglobin_latest         → c_hb        
  + int_platelets_latest           → high_platelet
  + int_lft_latest                 → high_lft    (composite of ALT, GGT, bilirubin)
  + int_falls_observations_all     → b_falls
  + int_malabsorption_diagnoses_all          → b_malabsorption
  + int_vte_diagnoses_all          → b_vte
  + int_liver_pancreatitis_diagnoses_all     → b_liverpancreas
  + int_person_geography           → town        (townsend_score)
  + qadmissions_alcohol_category         → alcohol_cat6
  + qadmissions_ethrisk        → ethrisk
  + constants                      → sha1 (=3, London), surv (=qadmissions_horizon_years var)
```

---

## Codelist strategy: 

The codelists used to define the features use a combination of the codelists defined in the Qadmissions research paper and codelists already present within the NCL dbt analytics framework.

All codelists from the qadmissions paper were downloaded from the Endeavour IM Directory `QPredict_334` set (5,537 codes across 51 codelists), uploaded to `DATA_LAKE__NCL.TERMINOLOGY.Q_ADMISSIONS`, and entered into the `DATA_LAKE__NCL.TERMINOLOGY.DEFINITION_STORE` schema. They are all appended with the `_COD_Q` suffix convention. 

The features split into four groups based on where their codes come from.

### 1. QAdmissions paper-faithful codelists (`*_COD_Q`)

- `b_falls` ← `FALLS_COD_Q`
- `b_malabsorption` ← `MALABSORPTION_COD_Q` (single combined cluster: coeliac + IBD + post-resection)
- `b_vte` ← `VENOUS_THROMBOEMBOLISM_COD_Q` (DVT + PE combined)
- `b_liverpancreas` ← `LIVER_DISEASE_OR_CHRONIC_PANCREATITIS_COD_Q` (liver disease and chronic pancreatitis combined)

### 2. NCL/QOF disease registers (via `dim_person_conditions`, `fct_person_diabetes_register`)

- `b_AF`, `b_CCF`, `b_anycancer`, `b_asthmacopd`, `b_epilepsy`, `b_renal`, `b_manicschiz`, `b_cvd` (composite)
- `b_type1`, `b_type2` (split via `fct_person_diabetes_register.diabetes_type`)

### 3. NCL clusters (not `_COD_Q`-suffixed) for value-based features

- Lab clusters used by the three lab features (`c_hb`, `high_platelet`, `high_lft`):
  `HAEMOGLOBIN_EST`, `PLATELET_COUNT`, `ALT_LEVELS`, `GGT_LFT_COD`, `BILIRUBIN_LEVEL`. Paper-faithful equivalents (`HAEMOGLOBIN_VALUE_COD_Q`, `PLATELET_VALUE_COD_Q`, `ALT_COD_Q`, `GGT_COD_Q`, `BILIRUBIN_COD_Q`) exist in `DEFINITION_STORE` and could be swapped in.
- Medication clusters used by the five `b_*` medication booleans (`b_anticoagulant`, `b_antidepressant`, `b_antipsychotic`, `b_corticosteroids`, `b_nsaid`) come from the existing `int_<med>_medications_all` intermediates and are NCL-defined.

### 4. Other project intermediates

- `bmi` ← `int_bmi_latest`, with missing values imputed (see "BMI imputation")
- `smoke_cat` ← `int_smoking_status_latest`
- `hes_admitprior_cat` ← `fct_person_sus_apc_recent.apc_nel_12mo`
- `town` ← `int_person_geography.townsend_score` (joins the postcode-derived `lsoa_code_11` to the `townsend_lsoa_2011` seed)
- `alcohol_cat6` ← `qadmissions_alcohol_category` (Full AUDIT scores from `int_alcohol_audit_scores`)
- `ethrisk` ← `qadmissions_ethrisk` (`dim_person_ethnicity.ethnicity_subcategory` joined to `qadmissions_eth2016_to_ethrisk9` seed)

---

## Unknowns from paper

Worth flagging for any future audit / clinical review.

### Liver function test thresholds and components

The paper says "a single variable which denoted either a high γ-GT, aspartate aminotransferase or bilirubin where a high value was at least three times the upper limit of normal". The paper does **not** define the upper limit of normal (ULN) values. Our `qadmissions_lab_thresholds` seed currently uses:

- ALT > 120 [IU]/L (3× a 40 ULN)
- GGT > 150 [IU]/L (3× a 50 ULN)
- Bilirubin > 63 µmol/L (3× a 21 ULN)

Sources for the 1× ULN values are noted as ["3x normal as per sps nhs resource"](https://www.sps.nhs.uk/articles/assessing-liver-function-and-interpreting-liver-blood-tests/). 

### Medication "current prescription" definition

The paper says "Current prescribed medication" but doesn't quantify it. The reference C implementation at `/c_pipeline/c/Q78_qadmissions_4_1.c` takes booleans, so the threshold has to be applied upstream by the caller. We use **≥1 prescription in the prior 6 months** via the `is_recent_6m` flag on the `int_<med>_medications_all` intermediates. The paper does not state this threshold; it's a project decision.

### Townsend score vintage

The paper used a Townsend score derived from the 2001 census. Our seed (`townsend_lsoa_2011`) uses 2011-census-derived TDS values per LSOA 2011 (UK-wide, ~42,619 LSOAs). The four Townsend component variables (unemployment, no-car ownership, non-owner-occupation, overcrowding) are defined consistently across the 2001 and 2011 censuses.

### "Most recent" lab value and ethnicity

The paper says lab values use "the most recently recorded value" and ethnicity is "most recently recorded ... in the study period before the patient had the outcome". No explicit time cap. Our `int_haemoglobin_latest`, `int_platelets_latest`, `int_lft_latest` and ethnicity sources mirror this — no time window.

### Alcohol coding

The paper uses six SNOMED-coded alcohol categories (`Non-drinker` through `Very Heavy >9 units/day`). We derive `alcohol_cat6` from **Full AUDIT scores** (`int_alcohol_audit_scores` filtered to `audit_type = 'Full AUDIT'`) with score bands mapped to 0..5 (0 / 1-3 / 4-7 / 8-15 / 16-19 / 20+). The AUDIT instrument is not the same as the SNOMED category codes the paper used. The six paper-faithful alcohol clusters (`ALCOHOL_NON_DRINKER_COD_Q` … `ALCOHOL_VERY_HEAVY_9U_DAY_COD_Q`) are already in `DEFINITION_STORE` and could be used instead. A coverage comparison (Full AUDIT vs SNOMED-category) has not been run.

### Smoking categories

The paper distinguishes light (1-9/day), moderate (10-19/day) and heavy (20+/day) smokers (categories 2..4). Our `smoke_cat` only emits 0 (never), 1 (ex), or 3 (current — light) because OLIDS does not record cigarettes-per-day. All current smokers are mapped to category 3.

---

## Resolution codes and time limits

### Resolution codes

None of the diagnosis intermediates (falls, malabsorption, VTE, liver-pancreatitis, plus the eight conditions sourced from `dim_person_conditions`) currently account for explicit resolution codes such as "VTE resolved" or "IBD in remission". The four `_COD_Q` clusters were scanned during Step 4 — they contain no resolution codes. The five "in remission" entries in `MALABSORPTION_COD_Q` are SNOMED `(disorder)` codes for active IBD with a remission qualifier, not resolution markers.

If the eight `dim_person_conditions`-sourced features are ever switched to QAdmissions clusters, resolution-code handling for those clusters would need to be re-evaluated.

### Time limits on diagnoses

Every diagnosis is "ever" — no time window applied. The `falls_flags` CTE in `qadmissions_input_features.sql` is a plain `SELECT DISTINCT person_id, TRUE FROM int_falls_observations_all`. The paper does not state a falls window in the methods text. Adding a 12-month window to `b_falls` (or to other diagnoses) would be a `WHERE clinical_effective_date >= DATEADD(day, -365, CURRENT_DATE())` in the relevant `_flags` CTE.

### Lab and observation time limits

`int_*_latest` models pick the most recent valid value with no time cap. The paper uses "the most recently recorded value" without specifying a cap. If a cap is wanted for clinical or data-quality reasons, it would be best added at the `_latest` layer (in `int_haemoglobin_latest`, `int_platelets_latest`, `int_lft_latest`) so other consumers see the same definition.

### Prior emergency admissions

Already capped at 12 months prior (per the paper's "emergency admissions in the year before") via `fct_person_sus_apc_recent.apc_nel_12mo`. No design choice to revisit.

---

## BMI imputation

A missing BMI is filled in before scoring. The QAdmissions paper used multiple imputation for missing BMI; this is a simpler single imputation.

- **Who:** everyone with no valid recorded BMI in `int_bmi_latest`. Recorded values are never changed.
- **The value:** the mean recorded BMI of eligible people with the same sex, age decade (`FLOOR(age / 10) * 10`) and ethnicity category from `dim_person_ethnicity`, with "Unknown" (a code with no category) counted as "Not Recorded".
- **Small groups:** if that group has fewer than 100 recorded BMIs (var `qadmissions_bmi_imputation_min_group`), the mean for sex and age decade is used, then the mean for sex.
- **Not clipped:** the means use every valid recorded BMI (10–150), and the aim is the most accurate BMI, so an imputed value outside 20–40 is left as it is and the model rejects that row.
- **Columns:**
  - `bmi` is what the model scores: recorded, or imputed
  - `bmi_recorded` keeps the recorded value
  - `is_bmi_imputed` and `bmi_imputation_group` show what was imputed and from which group
  - `qadmissions_predictions` carries all of them, plus `bmi_imputation_method`
- **History:** `qadmissions_input_features_validation` imputes the same way, with the means computed separately for each index date from that date's population and the BMIs known by then. Ethnicity is today's category at every index date, like `ethrisk`.
- **Code and tests:**
  - the logic is in `macros/clinical/qadmissions_impute_bmi.sql`
  - `tests/qadmissions_impute_bmi_synthetic.sql` checks it on made-up rows
  - the column tests check that recorded values are unchanged and that everyone gets a BMI
  - a `not_null_proportion` test on `bmi_recorded` (var `qadmissions_min_recorded_bmi_share`) fails if the BMI feed breaks, which would otherwise have everyone imputed
- **Switching it off:** set var `qadmissions_impute_bmi` to false. `bmi` is then the recorded value, and the tests still pass.

Why these choices: imputing from the mean was more accurate than from the median, on both BMI and the model's hazard. Adding the ethnicity category corrected the biggest group-level biases, such as BMI being underestimated for Black people when only sex and age were used.

---

## NULL handling

The model needs a value for every feature in order to score a row.

**Imputed when missing:**
- `bmi`: see "BMI imputation"

**NULL pass-through (no default applied):**
- `town`: rows without a Townsend score aren't scored

**Default applied when no record exists:**
- `alcohol_cat6` → `0` (NonDrinker), the category a Full AUDIT score of 0 gives
- `ethrisk` → `1` (NotRecorded, the same group as White)
- `hes_admitprior_cat` → `0`
- `smoke_cat` → `0`
- The eight `b_*` disease booleans (and the four `_COD_Q`-sourced ones) → `FALSE`
- The five medication booleans → `FALSE`
- `c_hb`, `high_lft`, `high_platelet` → `FALSE`

---

## Singular tests and data drift

The project has two singular tests guarding QAdmissions inputs:

- `tests/qadmissions_lab_thresholds_has_required_rows.sql` — fails if the `qadmissions_lab_thresholds` seed is missing one of the five required (measurement, direction) rows that downstream MAX(CASE) pivots silently return NULL for.
- `tests/qadmissions_ethnicity_mapping_complete.sql` — fails if `dim_person_ethnicity.ethnicity_subcategory` exposes a value not in the `qadmissions_eth2016_to_ethrisk9` seed. Without this, a new ethnicity value (e.g. a future ETH2016 cluster, or a renamed subcategory) would silently default to ethrisk = 1 and the model would underweight that group's risk.

Each `int_*_diagnoses_all` and `int_*_observations_all` model also has a `cluster_ids_exist` model-level test that fails if the named cluster is missing from the codeset reference data.

---

## Things that may change


1. **Add a 12-month window to `b_falls`** if the registered model under-fits or if clinical review prefers the recent interpretation. One-line change.
2. **Document the LFT ULN values** with explicit clinical references in `seeds/qadmissions_lab_thresholds.yml`.
3. **Promote `qadmissions_ethrisk` and `qadmissions_alcohol_category` out of the QAdmissions folder** (Townsend has already moved to `int_person_geography.townsend_score`) if any non-QAdmissions analysis wants them — generic equivalents under `models/modelling/olids/person_attributes/`.
4. **Decision on codelist usage**. Decide to either fully adopt the qadmissions codelists or create our own updated codelists for all features.


