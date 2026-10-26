# LTC LCS result headers

Some OLIDS investigations have an empty parent observation and a numeric child
with the same concept. Ranking both can select the empty header. The paired
dbt-OLIDS release identifies these records with `is_result_header`.

Staging exposes the boolean and retains every eligible observation. Only LTC LCS
result selection changes here. `get_ltc_lcs_observations` defaults to keeping
headers; its `exclude_result_headers` option filters them on both matching paths.
The latest-observation, latest-record-in-set and blood-pressure selectors enable
that option before ranking or linking. Inline numeric result pools enable it too,
including pools that already reject missing values, to make the intent explicit.
NAFLD uses save time before observation ID for same-date ties, as established by
the CKD and NAFLD EMIS alignment change.

A newer genuine result-less record still wins over an older numeric reading.
Header exclusion uses the existing thresholds, register definitions and code lists.

## Follow-up outside LTC LCS

`get_observations` does not yet expose the flag. A separate change can carry it
to result consumers and exclude headers before their own deduplication or
`get_latest_events` calls. The generic latest-event macro also selects diagnoses
and reviews, so its default behaviour must not assume numeric observations.

`int_blood_glucose_all` deduplicates by person, concept and date before
`int_blood_glucose_latest` ranks results. Both accept empty results. Confirmed
headers in the matched glucose pools give a bounded upper limit on potentially
affected people, rather than a measured count of changed latest results. Filter
headers before the first deduplication and replay `int_smi_six_health_check` and
`int_smi_hc_check_time_series` separately.

`get_lipid_observations` and the HbA1c, urine ACR and laboratory history models
could omit empty headers from measurement histories. Their latest-valid models
already reject missing numeric values, so removing headers has no direct effect
on which valid numeric reading wins. Review any history counts and latest-date
metadata separately. None of those shared macros or models changes here.

A mapped-concept match can flag a header whose child has a different source code.
An original-code-only value set can then exclude the header without admitting its
child, so selection falls back to an older eligible result. Check this edge case
in the follow-up; it does not change the structural flag rule.
