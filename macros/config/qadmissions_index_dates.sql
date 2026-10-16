/*
QAdmissions history index dates

qadmissions_history_index_dates() returns a query of the month-ends that
qadmissions_input_features_history is built for, as one reference_date column:
every month-end from August 2022 to July 2024. The model and
tests/qadmissions_features_history_index_dates_have_rows.sql both read it, so
the test always checks the dates the model uses.

The months are taken from int_date_spine, not from the person-month spine, so a
month that falls outside the person-month spine's rolling 60-month window (from
early 2028) still appears here and fails the test instead of silently dropping.

A month can be used for outcome validation only once it has a full outcome
horizon of complete SUS follow-up. The range ends in July 2024 so that every
date has complete 2-year follow-up (SUS complete to July 2026 at October
2026). It starts in August 2022 to keep 24 months, which stay inside the
person-month spine and register histories until about September 2027.

qadmissions_history_horizon_years() returns the prediction horizon, in years,
for validating against these index dates: 2, the horizon the QAdmissions paper
validated. qadmissions_input_features_history passes it to the model as surv,
and qadmissions_outcomes_history follows each person for the same horizon, so
the scores and outcomes always match.
*/

{% macro qadmissions_history_index_dates() %}
    SELECT month_end_date AS reference_date
    FROM {{ ref('int_date_spine') }}
    WHERE month_end_date BETWEEN '2022-08-31'::DATE AND '2024-07-31'::DATE
{% endmacro %}

{% macro qadmissions_history_horizon_years() %}
    {{ return(2) }}
{% endmacro %}
