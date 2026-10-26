# LTC LCS result headers

Some OLIDS investigations have an empty parent observation and a numeric child
with the same concept. Ranking both can select the empty header. The paired
dbt-OLIDS release identifies these records with `is_result_header`.

Staging exposes the boolean and retains every eligible observation. Only LTC LCS
result selection changes here. `get_ltc_lcs_observations` defaults to keeping
headers; its `exclude_result_headers` option filters them on both matching paths.
The latest-observation, latest-record-in-set and blood-pressure selectors enable
that option before ranking or linking. Inline numeric result pools enable it too.
NAFLD also uses save time before observation ID for same-date ties.

A newer genuine result-less record still wins over an older numeric reading.
The change uses the existing thresholds, register definitions and code lists.

## Deploy after the OLIDS release

1. Merge and deploy the paired dbt-OLIDS change. Build
   `int_observation_result_headers` and `stable_observation`.
2. Immediately republish the `DATA_LAKE.OLIDS` views. Confirm that
   `OBSERVATION.IS_RESULT_HEADER` exists and has no nulls.
3. Merge this change. Build `raw_olids_observation` and
   `stg_olids_observation`, then the affected LTC LCS models and their consumers.
   Use `dbt ls -s stg_olids_observation+` to inspect the full lineage. Other
   programmes' result-selection logic remains unchanged.

The existing OLIDS mapping in `scripts/sources/source_mappings.yml` already routes
the source to `raw_olids_observation`. No new mapping is needed. Source YAML and
raw SQL were generated with `2_generate_sources.py` and
`3_generate_raw_models.py` using the declared release column metadata. After
publication, normal metadata extraction can discover the same boolean.

## Validation before publication

The analytics `dev` target and production-state deferral compile the LTC LCS
selection. The native build cannot pass until the source column is published;
it fails at the generated raw view with `invalid identifier 'IS_RESULT_HEADER'`.
There is no silent false fallback that could leave headers eligible after deploy.

`ltc_lcs_result_headers_synthetic` tests both matching paths, default retention,
header exclusion and retention of a newer genuine empty result. The aggregate
replay uses the existing case-finding and MoC harnesses, baseline Git SQL and
changed worktree SQL against the same production inputs. Session temporary
relations contain the compiled OLIDS flag and exact generated raw/staging SQL.
Clinical-date and saved-date cut-offs apply in both periods.

All fetched replay results are aggregate counts. No seeds or production models
are rebuilt for this validation. The implementation report records the replay
results and the remaining native-build prerequisite.

## Follow-up outside LTC LCS

`get_observations` does not yet expose the flag. A separate change can carry it
to result consumers and exclude headers before their own deduplication or
`get_latest_events` calls. The generic latest-event macro also selects diagnoses
and reviews, so its default behaviour must not assume numeric observations.

`int_blood_glucose_all` deduplicates by person, concept and date before
`int_blood_glucose_latest` ranks results. Both accept empty results. The current
mapped glucose code pools contain 273,735 structurally confirmed headers across
153,276 people. That is an upper bound on people whose glucose selection could
change, not a measured number of changed latest results. Filter headers before
the first deduplication and replay `int_smi_six_health_check` and
`int_smi_hc_check_time_series` separately.

`get_lipid_observations` and the HbA1c, urine ACR and laboratory history models
could omit empty headers from measurement histories. Their latest-valid models
already reject missing numeric values, so removing headers has no direct effect
on which valid numeric reading wins. Review any history counts and latest-date
metadata separately. None of those shared macros or models changes here.
