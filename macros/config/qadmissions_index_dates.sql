/*
QAdmissions history index dates

qadmissions_history_index_dates() lists the month-ends that
int_qadmissions_features_history is built for. The model and
tests/qadmissions_features_history_index_dates_have_rows.sql both read it, so
the test always checks the dates the model uses.

Each date must be a month-end inside the rolling 60-month window of
int_segmentation_person_month_spine, or it matches no rows.

A date can be used for outcome validation only once it has a full outcome
horizon of complete SUS follow-up. 2026-09-30 is for comparison with the live
int_qadmissions_features only: it has no follow-up and must not be used for
outcome validation.
*/

{% macro qadmissions_history_index_dates() %}
    {{ return([
        '2022-01-31',
        '2023-01-31',
        '2026-09-30'
    ]) }}
{% endmacro %}
