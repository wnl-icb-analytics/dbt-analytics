# Imaging Turnaround Times (TAT)

Analysts should query `int_tat_turnaround_times`
(`MODELLING.DIAGNOSTICS.INT_TAT_TURNAROUND_TIMES`). That is the supported
modelled table.

Do not query `DATA_LAKE__NCL.ANALYST_MANAGED.TURNAROUND_TIMES_RAW`. That copy
is the retirement target. This project no longer treats it as an analyst
interface. Keep `DATA_LAKE.TAT.TURNAROUND_TIMES_RAW`. That is the ingest
landing table, not the old analyst-managed copy.

Provider submissions land in `DATA_LAKE.TAT` through the `tat_provider_ingest`
pipeline (Snowflake-Deployment). dbt then stages and models them here.

Lineage: provider files → `DATA_LAKE.TAT` (raw) → dbt raw → staging → modelling.

PRs: Snowflake-Deployment #23 (ingest) · dbt-analytics #818 (modelling).

## 1. Which table to use

| Role | Object | Status |
|---|---|---|
| Analyst query | `int_tat_turnaround_times` / `MODELLING.DIAGNOSTICS.INT_TAT_TURNAROUND_TIMES` | Supported |
| Typed source interface | `stg_tat_turnaround_times` | Supported for modelling and freshness, not the analyst table |
| Ingest landing | `DATA_LAKE.TAT.TURNAROUND_TIMES_RAW` | Keep. Feed for dbt raw. All-STRING 1:1 of provider files |
| Old analyst copy | `DATA_LAKE__NCL.ANALYST_MANAGED.TURNAROUND_TIMES_RAW` | Retired from this project. Do not drop until warehouse reads and writes are proven gone |

Two warehouse objects share the name `TURNAROUND_TIMES_RAW`. Mixing them up
gives different answers. The supported modelled grain is one diagnostic test
event (`tat_event_id`) in the surviving Flex or Freeze submission for each
trust and data period.

## 2. Why totals can differ

`int_tat_turnaround_times` is not a 1:1 copy of the analyst-managed table.

- Out-of-range Flex/Freeze rows are dropped. `datedifftest` is whole months
  between the file's data period and the test month: `-3` (or the hardcoded
  `20241028_DIDNCL_RAN_Jul24.csv` file) is Freeze, `-2` or `-1` is Flex,
  anything else is dropped.
- Restatement keeps the latest submission file per trust and data period
  (`submission_date`, then `loaded_at`, then `file_name`). Older Flex files
  for that trust and period do not survive.
- Rows with a missing test date or request date are dropped, matching the
  original R pipeline.
- Header variants are coalesced, UK and ISO datetimes are parsed, and the
  cancer pathway flag is standardised to Y / N / Unclassified.
- The old R upload also set `referring_organisation` to null before writing
  Snowflake. The dbt model keeps the coalesced referring organisation from
  the file.

Compare counts at trust and data-period level, not row-for-row. This dataset
is person-level. Keep GitHub evidence to aggregates and consumer names.

## 3. Field mapping

Snowflake folds unquoted identifiers. The old R upload used mixed-case names
such as `TAT_scan`. Use the snake_case names on `int_tat_turnaround_times`.

| Analyst-managed column | Supported column | Notes |
|---|---|---|
| `submission_date` | `submission_date` | Date from the filename prefix |
| `data_type` | `data_type` | `Freeze` or `Flex` only |
| `month` | `month` | 3-letter data-period month |
| `year` | `year` | 4-digit data-period year |
| `trust_code` | `trust_code` | Provider code from the filename |
| `data_period` | `data_period` | First day of the nominal data month |
| `ethnic_category` | `ethnic_category` | |
| `person_gender` | `person_gender` | |
| `general_medical_practice` | `general_medical_practice` | |
| `patient_source_type` | `patient_source_type` | Integer |
| `referrer_code` | `referrer_code` | |
| `referring_organisation` | `referring_organisation` | Populated in dbt; the old upload nulled it |
| `diagnostic_test_request_date_time` | `diagnostic_test_request_date_time` | |
| `diagnostic_test_request_received_date_time` | `diagnostic_test_request_received_date_time` | |
| `diagnostic_test_date_time` | `diagnostic_test_date_time` | |
| `service_report_issue_date_time` | `service_report_issue_date_time` | |
| `imaging_code_nicip` | `imaging_code_nicip` | Spaced and unspaced headers coalesced |
| `imaging_code_snomed` | `imaging_code_snomed` | Spaced and unspaced headers coalesced |
| `combined_imaging_code` | `combined_imaging_code` | NICIP, else SNOMED |
| `provider_site_code` | `provider_site_code` | Spaced and unspaced headers coalesced |
| `priority_type_code` | `priority_type_code` | Integer |
| `priority_type_code_routine_default` | `priority_type_code_routine_default` | Null defaulted to 1 |
| `cancer_pathway_flag` | `cancer_pathway_flag` | As submitted |
| `cancer_pathway_flag_string` | `cancer_pathway_flag_string` | Y / N / Unclassified |
| `TAT_scan` | `tat_scan` | Hours, request to scan. See rounding below |
| `TAT_report` | `tat_report` | Hours, scan to report. See rounding below |
| `TAT_overall` | `tat_overall` | Hours, request to report. See rounding below |
| `datedifftest` | `datedifftest` | Whole months, data period to test month |
| `file_name` | `file_name` | Surviving submission filename |
| `month_year` | *(derive)* | `to_char(data_period, 'MonYY')` |
| `submission_month` | *(derive)* | `month(submission_date)` |
| `submission_year` | *(derive)* | `year(submission_date)` |
| *(none)* | `tat_event_id` | Grain. Hash of filename, trust, test times, codes and demographics |
| *(none)* | `source_file` | Stage path |
| *(none)* | `loaded_at` | Raw load timestamp |

Rounding: dbt rounds TAT hours half away from zero. The R pipeline that wrote
the analyst-managed table rounded half to even, so a value exactly on a half
hour (for example 30 minutes) can be one hour higher in dbt. Under 1% of rows
are affected.

## 4. Consumers found in code

This list is from public git. Snowflake access history for the 90 days to
1 October 2026 shows the analyst-managed table last written on 13 August 2026
(the `tat_dashboard` delete-and-append) and last read on 27 August 2026. Use
access history over the agreed retirement period before dropping it (section 5).

### This repository

No model, seed, test or source YAML reads
`DATA_LAKE__NCL.ANALYST_MANAGED.TURNAROUND_TIMES_RAW`. It is not in
`models/sources/manual_analyst_managed.yml`. Do not add it.

`stg_source_content_freshness` reads `stg_tat_turnaround_times` (the supported
staging interface to `DATA_LAKE.TAT`). `int_tat_turnaround_times` has no
downstream dbt model.

### Other public repositories

`tat_dashboard` still writes the analyst-managed table from
`tat_dashboard_data_snowflake.qmd`: it deletes Flex rows for the submitting
trusts, then appends the new month. That notebook is the remaining known
writer. Move any dashboard or worksheet that reads the old table onto
`int_tat_turnaround_times`, then stop that write before anyone drops the
table.

SQL in that repository that names `Data_Lab_NCL_Dev` sandpit objects is a
separate SQL Server path, not this Snowflake table.

The supported ingest writer for new provider files is
`DATA_LAKE.TAT.SP_TAT_LOAD_RAW()` in Snowflake-Deployment. Leave that in
place.

## 5. Drop

Do not drop `DATA_LAKE__NCL.ANALYST_MANAGED.TURNAROUND_TIMES_RAW` in this
change. Drop only that analyst-managed table, through the approved Snowflake
process, once `SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY` shows no reads and no
writes for the whole retirement period agreed in #1150. A single zero-read
check can miss infrequent consumers. On 1 October 2026 the last write was
13 August 2026 and the last read 27 August 2026. Do not drop `DATA_LAKE.TAT.TURNAROUND_TIMES_RAW`.

## 6. Ingest objects in `DATA_LAKE.TAT`

Created by `sql/01_setup.sql` + `sql/03_procs.sql` in the `tat_provider_ingest`
pipeline.

| Object | FQN | Type | Purpose |
|---|---|---|---|
| Raw landing table | `DATA_LAKE.TAT.TURNAROUND_TIMES_RAW` | TABLE | All-STRING 1:1 landing of provider submissions |
| Ingest log | `DATA_LAKE.TAT.TAT_INGEST_LOG` | TABLE | Per-file load audit (status, rows, errors, who) |
| Stage | `DATA_LAKE.TAT.TAT_SUBMISSIONS` | STAGE | Transient upload pipe (`incoming/`, cleared after load) |
| File format | `DATA_LAKE.TAT.TAT_CSV` | FILE FORMAT | CSV parse (header, BOM, NA sentinels) |
| xlsx converter | `DATA_LAKE.TAT.SP_TAT_CONVERT_XLSX()` | PROCEDURE (Python) | Converts staged `.xlsx` → `.csv` |
| Raw loader | `DATA_LAKE.TAT.SP_TAT_LOAD_RAW()` | PROCEDURE (SQL) | Idempotent upsert by `SOURCE_FILE` (delete-then-reload), `SKIP_FILE` on bad files, write ingest log |
| Datetime parser | `DATA_LAKE.TAT.PARSE_TAT_TS(VARCHAR)` | FUNCTION | UK `DD/MM/YYYY HH:MI` (+ ISO) → `TIMESTAMP_NTZ` |

## 7. dbt models

Names are case-insensitive in Snowflake.

| Model | Layer | Prod FQN | Dev FQN |
|---|---|---|---|
| `raw_tat_turnaround_times_raw` | raw (view) | `STAGING.DBT_RAW.RAW_TAT_TURNAROUND_TIMES_RAW` | `DEV__STAGING.DBT_RAW.RAW_TAT_TURNAROUND_TIMES_RAW` |
| `stg_tat_turnaround_times` | staging (table) | `STAGING.TAT.STG_TAT_TURNAROUND_TIMES` | `DEV__STAGING.TAT.STG_TAT_TURNAROUND_TIMES` |
| `int_tat_turnaround_times` | modelling (table) | `MODELLING.DIAGNOSTICS.INT_TAT_TURNAROUND_TIMES` | `DEV__MODELLING.DIAGNOSTICS.INT_TAT_TURNAROUND_TIMES` |

- raw is a 1:1 passthrough with cleaned column names (generated).
- staging types and normalises: header-spelling variants coalesced, UK
  datetimes parsed, trust and period derived from the filename. Grain is
  `tat_event_id` (exact re-loads de-duplicated).
- modelling adds TAT hours, Flex/Freeze classification, the standardised
  cancer flag, and restatement to the latest submission per trust and data
  period.

## 8. Tests

| Test | Model | Column |
|---|---|---|
| `unique`, `not_null` | both stg + int | `tat_event_id` (grain) |
| `accepted_values` (`Freeze`, `Flex`) | int | `data_type` |
| row count ≥ 1 | both stg + int | - |

## 9. What to sense-check

- Counts from the original modelling validation (dev): raw about 9.30 million
  rows from 145 files. A 148-file run gave staging about 9.4 million and
  modelling about 9.2 million (Flex 6.1 million / Freeze 3.1 million), all 7
  trusts. Later
  builds change as new files arrive.
- Datetime parsing: providers send UK `DD/MM/YYYY HH:MI`; xlsx-converted
  files arrive ISO. About 99.8% of test datetimes parsed in that run; the
  rest were missing or blank.
- Flex/Freeze and restatement behave as in section 2.
- Known ingest gap: 3 historical NMUH files have blank header columns
  (`PARSE_HEADER` rejects them). They are skipped by `ON_ERROR=SKIP_FILE` and
  logged, not loaded (145 of 148 files). 4 xlsx months also had a same-named
  `.csv` (de-duplicated as the same trust and period).
