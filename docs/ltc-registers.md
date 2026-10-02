# LTC and QOF registers

Maintainer guide for upgrading the registers to a new QOF business rules version, or adding or retiring a register.
The [condition definitions](model_documentation/olids_ltc_condition_definitions.md) describe membership and code clusters.
The [monthly history reference](ltc-register-history.md) covers past month-ends and their limits.

## What's in the family

Reporting paths below start at `models/reporting/olids/disease_registers/`.
There are 42 live register facts: 23 QOF facts in `qof/` and 19 others.
The seed, `fct_person_ltc_summary` and the monthly history cover 39 condition codes; the seed labels 21 of them QOF.
CVD, obesity2 and QOF NDH/GDM have live facts and PIT views but sit outside the summary and history.
The summary's `NDH` is the clinical NDH cohort, not QOF NDH/GDM. HF3 is a flag in the HF model, not a separate code.

| Object | Grain | Path | Purpose |
|---|---|---|---|
| Live register fact | One qualifying person | `qof/fct_person_*_register.sql` or `fct_person_*_register.sql` | Current membership, with clinical dates and descriptive fields |
| QOF PIT view | One person with returned evidence at the configured date | `qof_pit/pit_*_register.sql` | Evaluate the macro at one date; filter `is_on_register = TRUE` when counting members |
| Rule macro | One person and reference date with returned evidence | `macros/qof_registers/calculate_*_register.sql`, `macros/ltc_registers/calculate_*_register.sql` | Rules used by PIT views, monthly history and reconciliation |
| Monthly register | One member and month-end | `history/fct_person_*_by_month.sql` | Membership for people alive and registered at each month-end |
| LTC summaries | One member and condition; history adds month-end | `fct_person_ltc_summary.sql`, `history/fct_person_ltc_summary_by_month.sql` | Union of the 39 registers with seed labels |
| Register status | One person and seed condition | `fct_person_ltc_register_status.sql` | Membership and separate age-based denominator eligibility |
| Conditions dimension | One person in `dim_person_demographics` | `dim_person_conditions.sql` | Flags and counts; collapses `CYP_AST` into `AST` and `LD_U14` into `LD` for counts |
| Condition episodes | One person, condition and episode number | `fct_person_condition_episodes.sql` | Event-based episodes without register age, medication and other QOF restrictions |
| Condition seed | One condition code | `seeds/ltc_register_denominator_rules.csv`, `.yml` | Names, domains, QOF labels and inclusive denominator age bounds |
| Reconciliation and coverage | Failures by register or condition code | `tests/ltc_register_fct_pit_reconciliation.sql`, `tests/ltc_register_history_covers_seed.sql` | Detect membership drift and missing history mappings |

## Where the rules live

Each rule exists twice: in the live fact SQL and in its `calculate_*_register` macro.
PIT views, monthly history and the reconciliation test call the macro; no live fact does.
Change both together. The reconciliation test fails when they disagree on membership.
CVD composes CHD and stroke/TIA in both places.

The two copies differ in inputs and dates:

- Macros read `int_*_all` modelling event models through `ref()`, never staging observations.
  Live obesity reads the person-level `int_bmi_qof` and `int_ethnicity_qof` instead.
- Live facts keep future-dated evidence; macros only count evidence known by the reference date.
- Live facts use `dim_person_age`. Macros derive age at the reference date from `dim_person_birth_death.birth_date_approx`
  as `FLOOR(DATEDIFF('month', birth_date_approx, reference_date) / 12)`.
- Live FH, gestational diabetes and NAFLD join `dim_person_active_patients`; other live facts use different demographic joins.
  Do not assume every live fact is limited to registered, living people.

`reference_date_expr` sets one date and defaults to `CURRENT_DATE()`. `reference_dates` replaces it with a query
returning a `reference_date` column; history passes `ltc_register_history_month_ends()`.
`ltc_register_known_by` counts an event once its clinical or order date and any non-null recorded date are on or before the reference date.

PIT views call `get_reference_date()`, a deprecated wrapper around `qof_reference_date()` in `macros/config/qof_config.sql`.
The `qof_reference_date` var in `dbt_project.yml` defaults to `2025-11-04`, the EMIS QOF extract date, and accepts a date or `CURRENT_DATE()`.
It controls PIT views only. Override it with `--vars` to evaluate a QOF year-end.

### Code clusters

`get_observations` and `get_medication_orders` resolve cluster membership through `stg_reference_combined_codesets`,
which reads `DATA_LAKE__NCL.TERMINOLOGY.COMBINED_CODESETS`. Register clusters come from three sources:

| Source | Clusters | Updated by |
|---|---|---|
| `PCD` | All register diagnosis, resolution and supporting observation clusters not listed below | NHS England Primary Care Domain refset release, loaded outside this repo |
| `ECL_CACHE` | QOF drug clusters (`ASTTRT_COD`, `EPILDRUG_COD`, obesity2's `STAT_COD`, `EZETIMIBE_COD`, `BEMPACID_COD`, `INCLISIRAN_COD`, `PCSK9I_COD`), plus local clusters `LIT_COD`, `HYPOTHY_COD`, `MASLD_DX_CODES`, `OA_COD` | ECL definitions in `DATA_LAKE__NCL.TERMINOLOGY.ECL_CLUSTERS`, refreshed weekly with `REFRESH_ECL_CLUSTER` |
| `LTC_LCS` | `SYSBP_COD`, `DIASBP_COD` (hypertension BP staging only) | LTC LCS value sets |

The PCD load does not include QOF's dm+d drug refsets, so each QOF drug cluster is an ECL cache of its v51 refset
(for example `ASTTRT_COD` is `^12463601000001108 {{+HISTORY-MAX}}`). Its content follows the drug refset releases;
on an upgrade, check that the refset IDs in `ECL_CLUSTERS` match the new spec.

`AST_COD`, `CHD_COD`, `CKD_COD` and `DMRES_COD` also exist under UKHSA sources with different content.
QOF register inputs pass `source='PCD'` for every PCD cluster, so they never mix sources. Never pin the ECL drug clusters to PCD.

A PCD release changes cluster membership without any SQL change. It reaches the registers when the modelling inputs are rebuilt.
Registers always use the latest codes, including for past reference dates.
`stg_reference_pcd_refset_latest` exposes the release date and version; `stg_reference_pcd_refset_snapshots` holds past memberships.
The combined-codeset tests check equality with the latest PCD membership and flag a change of more than 10% in rows or clusters against the prior snapshot.

`cluster_ids_exist` tests in modelling YAML check that each named cluster has codes, with the source filter the SQL uses.
They do not check clinical correctness or spec conformance. Register `meta.indicator.code_clusters` is descriptive metadata only.

## Spec versions

QOF business rules are versioned yearly: v50 for 2025/26, v51 for 2026/27, with point releases (v51.1, v51.2) during the year.
The QOF registers follow v51, checked field by field against the v51 rule and extraction tables.
Rules for AF, CHD, cancer, CKD, dementia, depression, diabetes, epilepsy, hypertension, LD,
osteoporosis, PAD, palliative care and RA did not change from v50 to v51.
v51 changes are implemented: asthma age 5, COPD disorder and administrative clusters, HF3 reduced ejection fraction,
SMI without the lithium route, QOF NDH/GDM, obesity2 and CVD.
Obesity reads ethnicity from the `ETHALL*_COD` clusters, as both v50 and v51 specify; it previously used `ETH2016*_COD`.
With a 12-month BMI window this takes a QOF-style obesity register from 94.8% to 99.1% of the published 2025/26 register.

Models in `qof/` have macros in `macros/qof_registers/`; others in `macros/ltc_registers/`.

| Code | Live model | QOF | Basis | Rule |
|---|---|---|---|---|
| AF | `qof/fct_person_atrial_fibrillation_register` | Yes | v51 | Unresolved AF |
| AST | `qof/fct_person_asthma_register` | Yes | v51 | Aged 5 or over; medication in the preceding 12 months |
| CAN | `qof/fct_person_cancer_register` | Yes | v51 | Latest first/new episode on or after 1 April 2003 |
| CHD | `qof/fct_person_chd_register` | Yes | v51 | Any diagnosis |
| CKD | `qof/fct_person_ckd_register` | Yes | v51 | Aged 18 or over; stage 3 to 5, not downstaged or resolved |
| COPD | `qof/fct_person_copd_register` | Yes | v51 | Disorder codes at any date; administrative codes in the preceding two years |
| DEM | `qof/fct_person_dementia_register` | Yes | v51 | Any diagnosis |
| DEP | `qof/fct_person_depression_register` | Yes | v51 | Aged 18 or over; latest first/new episode since 1 April 2006 |
| DM | `qof/fct_person_diabetes_register` | Yes | v51 | Aged 17 or over; unresolved |
| EP | `qof/fct_person_epilepsy_register` | Yes | v51 | Aged 18 or over; medication in the preceding six months |
| HF | `qof/fct_person_heart_failure_register` | Yes | v51 | Unresolved HF; HF3 flag needs reduced ejection fraction, not LVSD alone |
| HTN | `qof/fct_person_hypertension_register` | Yes | v51 | Unresolved diagnosis; no age limit |
| LD | `qof/fct_person_learning_disability_register` | Yes | v51 | Diagnosis not followed by a removal code |
| OB | `qof/fct_person_obesity_register` | Yes | Local | Latest valid BMI at any date, not QOF's 12-month window |
| OST | `qof/fct_person_osteoporosis_register` | Yes | v51 | Two age and fracture cohorts; DXA required at ages 50 to 74 |
| PAD | `qof/fct_person_pad_register` | Yes | v51 | Any diagnosis |
| PC | `qof/fct_person_palliative_care_register` | Yes | v51 | Inclusion since 1 April 2008; no later "no longer indicated" code |
| RA | `qof/fct_person_rheumatoid_arthritis_register` | Yes | v51 | Aged 16 or over |
| SMI | `qof/fct_person_smi_register` | Yes | v51 | MH diagnosis; remission keeps membership; lithium is descriptive only |
| STIA | `qof/fct_person_stroke_tia_register` | Yes | v51 | Any stroke or TIA diagnosis |
| CD_REG | `qof/fct_person_cvd_register` | Yes | v51 | CHD or stroke/TIA; outside summary and history |
| OBES2_REG | `qof/fct_person_obesity2_register` | Yes | v51 | Aged 18 or over; BMI 35, or 32.5 for lower-threshold ethnicities, plus four of five comorbidities; outside summary and history |
| NDH_REG | `qof/fct_person_qof_ndh_gdm_register` | Yes | v51 | NDH aged 18 or over, or GDM at any age, with ordered diabetes-history rules; outside summary and history |
| NDH | `fct_person_ndh_register` | No | Local | Clinical NDH aged 18 or over, no unresolved diabetes; seed labels it QOF |
| FH | `fct_person_familial_hypercholesterolaemia_register` | No | Local | Any diagnosis at any age |
| CYP_AST | `fct_person_cyp_asthma_register` | No | Local | QOF asthma rule for under-18s, no lower age bound |
| LD_U14 | `fct_person_learning_disability_register_under_14` | No | Local | LD rule for under-14s |
| FRAIL | `fct_person_frailty_register` | No | Local | Coded mild, moderate or severe frailty, not eFI/eFI2 |
| ANX | `fct_person_anxiety_register` | No | Local | Unresolved diagnosis |
| ADHD | `fct_person_adhd_register` | No | Local | Diagnosis later than remission |
| CLD | `fct_person_chronic_liver_disease_register` | No | Local | CLD or cirrhosis diagnosis |
| THY | `fct_person_hypothyroidism_register` | No | Local | `HYPOTHY_COD` or legacy `THY_COD` |
| NAFLD | `fct_person_nafld_register` | No | Local | Any `MASLD_DX_CODES` diagnosis |
| GESTDIAB, PD, CEREBRALP, MND, MS, AUTISM, OA, SCD, THAL | `fct_person_<condition>_register` | No | Local | Any diagnosis |

### Reading the spec in SQL

Points to keep when changing a rule:

- **Dates, not timestamps.** Prepared observation dates are timestamps and can carry a time, because `get_observations`
  substitutes the recorded timestamp when a clinical date is later. Membership comparisons cast to `DATE`.
- **Same-day resolution.** Read literally, fields such as `AFIBRES_DAT = Latest > AFIBLAT_DAT` keep a person whose resolution
  is on the same day as their latest diagnosis. Published QOF does not: against the 2025/26 practice registers, treating a same-day
  resolution as resolving fits better (AF: 86 practices match exactly, against 73), most likely because GP systems order entries
  within a day and a resolution follows its diagnosis. AF, asthma, CYP asthma, depression and epilepsy therefore need the latest
  diagnosis after the latest resolution. CKD, diabetes, heart failure, hypertension and palliative care keep same-day membership;
  published QOF gives no clear signal for them. LD and COPD follow their own field operators.
- **Windows.** `> (ACHV_DAT - 12 months)` excludes the boundary day: `order_date > DATEADD('month', -12, reference_date)`.
- **Evidence up to the reference date.** Live facts and macros only count evidence dated on or before the date evaluated.

Where the spec cannot be applied literally:

- **Episode type.** Cancer and depression use the latest "first or new" episode. Records with episode type "None" count as first or new:
  that matches published QOF (99.7% and 99.9% of the 2025/26 registers), while excluding them gives 97.3% and 93.9%.
- **Age.** OLIDS gives an approximate birth date (mid-month), so age in full years can be a month early or late.
- **Patient-table ethnicity.** Obesity's `ETHBAMEPAT_ETHNIC` fallback reads the Patients table, which OLIDS does not expose. Journal ethnicity only.
- **Retired codes.** Some inputs use `include_history=true` to add retired SNOMED predecessors of cluster codes.
- **Spirometry units.** FEV1/FVC values above 1 are read as percentages before the 0.7 test.

## Deliberate differences from QOF

- **Obesity** uses the latest valid BMI (5 to 400 kg/m² inclusive) at any date: 30 kg/m², or 27.5 kg/m² when a lower-threshold
  ethnicity is recorded on the date of the latest ethnicity record (the v51 rule). QOF `OBES_REG` uses a qualifying BMI in the preceding 12 months. A person does not leave the register for not being weighed,
  so the local register sits well above QOF. A later, lower BMI removes them.
- **FH** is not a QOF register. Any `FHYP_COD` diagnosis counts at any age, matching how QOF v51 and the NICE exclusions read the cluster.
- **COPD** follows v51: `COPDDIAG_COD` at any date, `COPDPROC_COD` dated within two years before the reference date.
  Rule 4 admits remaining unresolved patients diagnosed from 1 April 2023 without spirometry or `SPIRPU_COD`.
  Years published under v50 lacked the administrative codes, so published registers for those years are smaller.
- **CYP asthma** applies the asthma rule to under-18s with no lower age bound, so it overlaps QOF asthma at ages 5 to 17.
  **LD under 14** is a subset of the all-age LD register.
- **Clinical NDH** excludes GDM-only patients and anyone with unresolved diabetes. It is not the QOF v51 NDH/GDM register.
- **Seed ages are denominators, not membership.** The seed sets asthma 6, FH 20 and hypertension 18; membership uses 5, any age and any age.
  `fct_person_ltc_register_status` applies them to `is_eligible_denominator` only.

## Upgrade to a new QOF version

1. Read NHS England's business rules, change log and tracked changes for the new version, including point releases.
   Compare extraction field definitions as well as rule tables: in v51 COPD's fields changed while its four rules did not.
2. Membership-only cluster changes arrive with the PCD release. Confirm the release in `stg_reference_pcd_refset_latest`,
   then rebuild the modelling inputs and consumers. No SQL change.
3. Check the refset IDs of the ECL drug clusters in `ECL_CLUSTERS` against the new spec's cluster table.
4. For new, renamed or retired clusters, change the `int_*_all` extraction, source filters, `cluster_ids_exist` tests and register metadata together.
5. For rule changes, edit both the live fact and its macro. Update SQL comments, YAML descriptions, this guide and the condition definitions.
   Read every extraction field's operator; see [reading the spec in SQL](#reading-the-spec-in-sql).
6. For a new or retired summary condition, update the seed and its YAML, `fct_person_ltc_summary`, `ltc_register_history_models()`
   and the monthly SQL/YAML pair. Add or retire the live fact, macro and QOF PIT view together.
   Update the reconciliation test's `register_pairs`, `register_sources` and population lists.
7. Check `dim_person_conditions` flags and count grouping, and `fct_person_condition_episodes` if its input clusters change.
8. Run `dbt ls -s <model>+` to see consumers. The main ones are NICE measures, segmentation, LTC LCS case finding and
   risk stratification, C-LTCS, population health needs and vaccination cohorts.
9. Say in the PR that history changes: today's rules and codes are applied to all 60 month-ends, and no earlier rule version is kept.
10. Build the changed models and their consumers on the `dev` target. Run the reconciliation, cluster, grain and history-coverage tests,
    plus the CVD, HF, NDH and obesity2 singular tests that apply.
11. Evaluate the PIT views at 31 March with `--vars` and compare register sizes with published QOF for the same practices, as percentages.
    Separate known definition gaps and record the PCD release used.

## Validation

`tests/ltc_register_fct_pit_reconciliation.sql` compares all 42 live facts with their macros, plus the HF3 flag, in both directions.
It evaluates the macros at the date the live table was built (the earlier of its build date and `dim_person_age`'s, via `ltc_live_register_as_of`).
It excuses people with evidence in that register's own sources dated or entered after that date, and aligns the macro population to the live demographic joins.
It checks membership only, not descriptive fields or past months.
Other singular tests cover the CVD union and qualifiers, HF same-day retention, clinical NDH against the summary,
GDM-only exclusion from clinical NDH, and obesity2's BMI, comorbidity and dyslipidaemia evidence.

Register sizes in the monthly history at 31 March 2026 against the published 2025/26 QOF registers, NCL practices
(history as a percentage of QOF; share of practices with a register of 20 or more within 5%):

| Register | % of QOF | Practices within 5% | | Register | % of QOF | Practices within 5% |
|---|---|---|---|---|---|---|
| AF | 99.7% | 98% | | HTN | 99.9% | 98% |
| Asthma | 100.1% | 99% | | LD | 101.0% | 89% |
| Cancer | 99.7% | 98% | | Obesity | 186.2% | 0% |
| CHD | 100.0% | 99% | | Osteoporosis | 102.9% | 79% |
| CKD | 100.3% | 96% | | PAD | 100.1% | 93% |
| COPD | 106.6% | 57% | | Palliative care | 102.1% | 76% |
| Dementia | 103.6% | 67% | | RA | 100.1% | 94% |
| Depression | 99.9% | 96% | | SMI | 99.2% | 94% |
| Diabetes | 99.7% | 99% | | Stroke/TIA | 100.1% | 98% |
| Epilepsy | 99.8% | 96% | | | | |

Known gaps:

- COPD is about 7% over from 2023/24: the v51 administrative codes count, while published years used v50. Administrative-only members are 6% to 11% of the register.
- Obesity is over by design (any-date BMI rule). With a 12-month window it is 99.1% of QOF.
- Dementia, osteoporosis and palliative care are 2% to 4% over, with wide practice spread. Their rules match v50 and v51 exactly, so the cause is in the data or cluster content; not yet explained.

Earlier years run lower because of registration history: the person-month spine was 97.5% of QOF list size in 2022 and 99.7% in 2026,
and the known-by rule removes a further 1% to 3% in 2022 (diagnoses entered after the year-end).
Heart failure in 2022 to 2024 and depression in 2023/24 reflect QOF definition changes in those years.
Re-derive these figures after an upgrade rather than treating them as tolerances.
