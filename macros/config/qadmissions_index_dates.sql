/*
QAdmissions history index dates

qadmissions_history_index_dates() returns a query of the month-ends that
qadmissions_input_features_history is built for, as one reference_date column:
every month-end from January 2023 to December 2024. The model and
tests/qadmissions_features_history_index_dates_have_rows.sql both read it, so
the test always checks the dates the model uses.

The months are taken from int_date_spine, not from the person-month spine, so a
month that falls outside the person-month spine's rolling 60-month window (from
early 2028) still appears here and fails the test instead of silently dropping.

A month can be used for outcome validation only once it has a full outcome
horizon of complete SUS follow-up.
*/

{% macro qadmissions_history_index_dates() %}
    SELECT month_end_date AS reference_date
    FROM {{ ref('int_date_spine') }}
    WHERE month_end_date BETWEEN '2023-01-31'::DATE AND '2024-12-31'::DATE
{% endmacro %}
