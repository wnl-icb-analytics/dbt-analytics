# Monthly SUS reporting

## Agreed purpose and schema

On 25 November 2026, Dharmesh confirmed the decision from his meeting with Ibrahim Somalya and Eddie Davison: retain a separate monthly SUS reporting version and publish its reporting models in `ACUTE_MONTHLY`. This records the design decision, not final technical approval or analyst sign-off.

The monthly interface supports historical monthly-feed analysis and ERNI migration reconciliation. It does not recreate every ERNI table or business rule. Established `ACUTE` OBTs remain the supported daily/weekly interface for their documented use cases. Monthly and OBT results are not interchangeable without aligning populations, dates and commissioner rules.

| Reporting table | Grain | Activity-date basis |
|---|---|---|
| `fct_sus_op_monthly_appointment` | One row per supplied OP appointment, including attendance, DNA and cancellation | Appointment date |
| `fct_sus_ae_monthly_attendance` | One row per supplied AE attendance | Arrival date |
| `fct_sus_apc_monthly_episode` | One row per supplied APC source encounter/episode | Episode end date |
| `fct_sus_apc_monthly_spell` | One row per supplied APC encounter selected by `dv_is_spell = 1` | Spell discharge date |

In DEV these tables are under `DEV__REPORTING.ACUTE_MONTHLY`; the production target is `REPORTING.ACUTE_MONTHLY` after review, merge and deployment. They contain activity-level records, not monthly aggregates. They inherit table materialisation from reporting. Raw and staging remain views.

The reporting folder carries `monthly-full`, not `daily`. The existing scheduled workflow excludes this tag from daily and weekly runs and includes it in the monthly full refresh. Adding a `monthly` tag alone would not achieve that separation. Source updates appear in these tables after a successful build; a schedule is not evidence of source freshness.

## Population, keys and field choices

The facts retain the full available source history and all administrative categories, including private activity. They do not apply ERNI commissioning-access exclusions. The sources do not expose separate Current, Reconciliation or Post-Reconciliation populations. They are not rolling two-month feeds.

The spell fact filters before left-joining the one-row-per-encounter PBR source. Unmatched spells are retained. The encounter key is `sk_encounter_id`; the supplied spell identifier is not a unique row key.

All four staging interfaces use `consistent_sk_patient_id_format`. The shared pseudonym key is trimmed text; blank values and the unknown-patient placeholder `1` become null, while leading zeroes are preserved. `dw_nhs_number` and `ccg_nhs_number` are retained only in source/staging evidence, not exposed in reporting.

Common reporting fields are `activity_date`, `financial_year`, `organisation_id`, `organisation_name`, `submitted_commissioner_code` and `submitted_commissioner_name`. Financial year uses the same established NHS financial-year macro as SUS reporting (for example `202526`). The source financial-period fields remain separately identifiable.

Organisation names are latest-available exact-code labels from the maintained `organisation` reference. They are not historical names at the event date and do not roll sites into parent providers. No missing lookup removes an activity row.

The monthly commissioner is explicitly the submitted source commissioner. Established OBTs use CAM-first assignment. A WNL filter on these two fields may select different activity. The monthly release does not silently introduce a new commissioner assignment rule.

Legacy `pct_` and `sha_` reporting names have been replaced by neutral commissioning-area names, retaining source lineage in YAML. These values are not represented as a current PCT/SHA hierarchy. Original, derived and validated practice fields remain distinct; there is no unapproved practice-selection rule.

## Source generation and national definitions

Sources are generated from live column metadata and `source_mappings.yml`, one file per schema: `auto_data_lake_sus_ae.yml`, `auto_data_lake_sus_op.yml` and `auto_data_lake_sus_ip.yml`. Allowlists select the three encounter date-range inputs and the IP PBR date-range input. Generated raw models are not hand-edited.

The [national SUS+ extract specification v3.12](https://digital.nhs.uk/binaries/content/assets/website-assets/services/sus/sus-guidance/sus_extract_specification-v-3.12.xlsx), cited by Eddie, provides the standard PBR field definitions. YAML now distinguishes national stay durations, critical-care-day measures, initial price, per-day excess-stay rate, excess-stay payment, pre-MFF subtotal and final national payment. These are supplied price components, not actual provider costs or evidence of an agreed invoice. No monetary rescaling is performed.

The national EM extract marks Conclusion Waiting Time, Duration Time and Treatment Wait Time as obsolete placeholders. Their local source variants remain in staging for lineage, with explicit warnings, and are omitted from monthly reporting. They must not be assigned minute-based threshold meanings merely from their names.

Local `dv_` rules, including spell selection and numeric period/date encoding, still need upstream confirmation. The numeric PBR `ActivityPeriod` is not equated with the nationally defined formatted `Applicable Costing Period`, applicable date or extract date. It remains a staging-only provenance field pending a definition. Do not infer missing units, scaling or special/null meanings from types or observed values.

No legacy `z` rules are reproduced. All-null PBR fields identified in the original inspection remain omitted. The [legacy field assessment](sus-monthly-legacy-field-assessment.md) records the original comparison and outstanding evidence; its counts describe that assessment, not the renamed final interface or an obligation to recreate ERNI columns.

## Validation and known differences

On 25 November 2026 the schema move passed the 12-model DEV build and all 16 key tests. The final interface rebuild passed all eight affected models and the same 16 tests. All four reporting objects are physical tables in `DEV__REPORTING.ACUTE_MONTHLY`, have the `monthly-full` tag without `daily`, and retain their previous row totals. Source-preservation checks returned zero discrepancy groups. Patient keys contain no blank or `1` placeholders.

The independent provider/month comparison covers April 2020 onwards. Across months through September 2026, with all commissioners included, monthly totals are below the established comparator by approximately 0.40% for AE, 0.57% for OP, 0.72% for APC episodes and 0.64% for APC spells. These are net count differences, not record-match rates or an approved tolerance. Provider-code variants and source population differences need interpretation.

The baseline DEV build passed all 12 models and the reporting/PBR key tests; the subsequent six staging key tests also passed. `sus_monthly_reconciliation.sql` checks that source/staging rows are preserved in each monthly fact. It is a copy/grain assurance check, not a test for activity missing upstream.

`sus_monthly_vs_established_reporting.sql` independently compares raw provider-code/month counts with `obt_appointment_op`, `obt_encounter_uec` and `obt_encounter_apc`. There is no established episode OBT, so APC episodes are explicitly compared with `stg_sus_apc_spell_episodes`. It includes all commissioners to avoid disguising submitted-versus-CAM selection differences. AE versus ECDS scope and provider/site code variants still require interpretation; equal counts do not prove record equality. Months beyond the monthly feed's latest coverage are flagged rather than treated as a newly discovered source gap.

The historic ERNI comparisons use the same nine commissioner codes on both systems, retain private activity, omit access exclusions and compare ERNI Current facts rather than final consolidated outputs. They support investigation, not full replacement sign-off.

Validation on 23 November found latest activity through 30 September 2026 in all four monthly facts. The feed can lag daily/weekly reporting by about two months. Latest source coverage must be checked before interpreting recent trends.

RV3 August 2026 OP activity is a separate upstream issue. The monthly source held 56 rows across all commissioners, with 6 in the nine-code comparison cohort, versus 41,732 and 32,956 respectively in ERNI Current at the earlier capture. Eddie reports the gap affects both Snowflake feeds. This release does not repair an ingestion gap.

## Remaining delivery gates

The superseded four DEV FCT objects in `ACUTE` were retired after checking visible dependent DEV views and confirming monthly count equality with their replacements. The earlier DEV-only monthly OBT/INT models are outside this PR.

Obtain Eddie's review of the agreed separate-schema implementation, upstream confirmation for required local derivations, Julie's agreed reconciliation/acceptance and production deployment evidence before claiming final delivery. PRs #999 and #947 contain wider business-rule/ECDS work; overlap alone is not sufficient evidence to discard their unique changes.
