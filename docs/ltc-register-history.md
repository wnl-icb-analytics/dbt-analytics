# Monthly LTC register history

`models/reporting/olids/disease_registers/history/` holds 39 monthly register tables and `fct_person_ltc_summary_by_month`.
They evaluate the live register rule family at past month-ends, then retain people alive and registered on those dates.
See [LTC and QOF registers](ltc-registers.md) for rule locations, spec versions, local differences and the upgrade checklist.

## Rows, population and dates

| Object | One row represents | Contents |
|---|---|---|
| `fct_person_*_by_month` | One register member at one month-end | `person_id`, `month_end_date`, `practice_code`, clinical dates and condition fields |
| `fct_person_ltc_summary_by_month` | One member, month-end and condition code | The 39 registers with seed name, domain and QOF label |
| `int_segmentation_person_month_spine` | One person in `dim_person_demographics` at one retained month-end | Birth, death, registration, age and practice state |

Monthly register tables contain members only. An absent row can mean no membership or no active spine row.
Use the spine for population denominators, not the number of people in the register summary.
The summary includes clinical NDH, not QOF NDH/GDM. It does not include CVD or obesity2.
HF history retains `is_on_hfref_register` within the HF table. There is no separate HF3 condition code.
Summary `is_qof` comes from the seed; its `NDH` value is true despite clinical NDH's live YAML being non-QOF.

`ltc_register_history_month_ends()` in `macros/ltc_registers/ltc_register_reference_dates.sql` returns the spine's
month-ends for the last 60 completed months. It bounds history even if an incremental spine run has kept older months.
It does not limit how old the clinical evidence for a month can be.

The window is 60 months because records for people who left or died are kept for five years upstream.
Earlier months would undercount people whose records are gone.

History joins the register's `reference_date` to `int_segmentation_person_month_spine.month_end_date` and requires `spine.is_active`.
In `models/modelling/segmentation/int_segmentation_person_month_spine.sql`, active means both alive and registered at month-end:

- Birth date is on or before month-end, with no death date on or before it.
- Registration starts on or before month-end and ends after it, or has no end date.
  An end before its start is treated as open, matching the spine's registration rule.

A person who dies or deregisters during the month is absent at that month-end.
This differs from a population defined by any registration overlap during a month.
The population begins with the current `dim_person_demographics` population, so it cannot recover people absent from that source.

## Practice at month-end

Practice comes from the spine's join to `dim_person_demographics_historical`, whose periods include the start and exclude the end.
For overlapping registrations, `dim_person_demographics_historical` orders by `is_current_registration DESC`, then `registration_start_date DESC`.
An open current registration takes precedence over a closed one. A past month can therefore show a later practice.
Practice-level comparisons inherit that limitation even when total membership agrees.

## When evidence becomes known

`ltc_register_known_by` requires the clinical or order date and any non-null recorded date to be on or before the reference date.
Both dates are cast to `DATE`. With a null recorded date, the clinical or order date controls entry.
A diagnosis dated earlier but entered later first counts at a month-end on or after the recorded date.
The rule applies to supporting medication and observations as well as diagnosis and resolution codes.

The clinical date is the prepared input date. `get_observations` substitutes the recorded date when the clinical date is later,
and uses `1900-01-01` for a null clinical date. Register comparisons cast these to `DATE`.
These models reconstruct known evidence from today's retained records. They do not preserve a past extract before corrections or deletions.

Age-restricted rules use the approximate birth date to derive age at each reference date.
Rolling medication and administrative-code windows also move with that date.
Earliest and latest diagnosis fields follow each register's definition, rather than a shared episode definition.
For obesity, `earliest_diagnosis_date` is the latest valid BMI date and `latest_diagnosis_date` is the latest BMI date of any kind.
For cancer, dates cover qualifying first/new episodes, which can include an earlier episode before the 1 April 2003 inclusion cutoff.

### Carry forward the latest known record

Obesity uses `ltc_latest_known_record` in `macros/ltc_registers/ltc_latest_known_record.sql` for BMI and ethnicity.
`ltc_known_date` takes the later clinical and recorded date, using the clinical date when recorded date is null.
The caller builds a sortable key; the highest key is the latest record. BMI keys combine clinical time, observation ID and cluster.
Ethnicity keys are the clinical date alone, because the v51 rule compares only the dates of the latest ethnicity and latest lower-threshold ethnicity records.

The helper orders records by known date and carries the highest key forward until the next known date.
It joins each interval to the reference dates within it, rather than joining every event to every later month.
This reduces repeated event-to-month work while retaining the latest clinical record known at each date.
A late-entered older record does not displace a newer clinical record. Obesity computes latest valid and latest-any BMI, and latest any and lower-threshold ethnicity, separately.

## Rebuilds and schedules

Every monthly register and the monthly summary is a table clustered by `month_end_date, person_id`, tagged `monthly-full`.
Each build replaces the whole table; there is no incremental append.
Newly entered, corrected or deleted source evidence can change past membership.

`.github/workflows/dbt-scheduled.yml` excludes `tag:monthly-full` from daily and weekly builds,
so history rebuilds in the monthly full refresh on the 1st, or in a targeted build.
The spine is incremental with `delete+insert`; a full refresh trims its aged-out months.

### After a spec upgrade

Today's rule macros and code lists apply to every past month, so a rule change or PCD release rewrites all 60 months on the next build.
The series shifts as a whole rather than stepping at the upgrade date, and no copy under the previous rules is kept.
Snapshot any series a consumer needs under the old rules before merging the upgrade.

## Tests and recorded validation

Each monthly register YAML tests uniqueness of `person_id, month_end_date` and non-null person/date keys.
Summary YAML tests `person_id, month_end_date, condition_code`, condition-code relationships to the seed,
and at most 60 distinct month-end dates. The date-count test does not prove that all 60 months exist for every condition.
`tests/ltc_register_history_covers_seed.sql` checks the seed codes and `ltc_register_history_models()` mapping in both directions.
It checks mapping coverage, not non-empty membership at every date.

`tests/ltc_register_fct_pit_reconciliation.sql` checks live facts against the same macros at their build-date reference,
with population alignment and register-source exceptions for evidence not yet known. It does not directly compare monthly tables with live facts.
See the [validation method and published-QOF table](ltc-registers.md#validation) for the populations and expected gaps.

At release (PR #1278), latest-month membership was 98.4% to 100.6% of the live registers on active patients.
All 39 conditions had 60 months with no null or out-of-order dates.
The largest month-on-month step was under 4% for 38 conditions, with one 6% osteoporosis step.
Early history has lower registration coverage: the spine was 97.5% of published QOF list size in 2022 and 99.7% in 2026.
Late-entered diagnoses removed a further 1% to 3% in 2022 under the known-by rule.
COPD and obesity also have the definition gaps described in the maintainer guide.

## Add a register to history

1. Establish the condition code, membership rule, input event models and live fact. Add its macro with the same rule,
   known-by filtering and age at reference date. Add a PIT view if it is a QOF register.
2. Add the condition to `seeds/ltc_register_denominator_rules.csv` and the live `fct_person_ltc_summary` union.
   Agree whether it belongs in the summary. CVD, obesity2 and QOF NDH/GDM currently do not.
3. Copy a neighbouring history SQL/YAML pair. Call the macro with `reference_dates=ltc_register_history_month_ends()`,
   join the active spine, retain practice and the summary's diagnosis dates, and add the person/month grain test.
4. Add its code/model pair to `macros/ltc_registers/ltc_register_history_models.sql`.
   The monthly summary reads that mapping and seed metadata automatically.
5. Extend reconciliation pairs, source lists and population alignment. Check seed coverage, cluster tests and grain tests.
   Update condition flags/counts and the [condition definitions](model_documentation/olids_ltc_condition_definitions.md) where the new condition belongs.
6. Build the changed history and summary on the tracked `dev` target and existing `DEV__` layers.
   Compare safe aggregate membership at the latest month and QOF year-end, and inspect steps across the window.

## Limits when interpreting a trend

Changes can reflect current rule versions, current cluster membership, late-entered evidence, registration coverage or practice overlap choices.
History is not the published register under the rules and code release used in each historical year.
Membership alone is not an incidence measure, and the summary's diagnosis dates are not uniformly onset dates.
Segmentation history and `person_month_analysis_base` do not yet read these tables; re-pointing them is a PR #1278 follow-up.
