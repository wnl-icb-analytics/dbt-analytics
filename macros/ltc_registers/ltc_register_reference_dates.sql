{% macro ltc_register_reference_dates(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
{#-
    Reference dates for the register macros, as a query returning one
    reference_date column.

    Args:
        reference_date_expr  SQL date expression evaluated as a single reference
                             date when reference_dates is not given
        reference_dates      query returning a reference_date column; every date
                             it returns is evaluated
-#}
{%- if reference_dates -%}
    {{ reference_dates }}
{%- else -%}
    SELECT {{ reference_date_expr }} AS reference_date
{%- endif -%}
{% endmacro %}


{% macro ltc_register_history_month_ends() %}
{#-
    Month-ends evaluated by the monthly register history: the completed
    month-ends held by the person-month spine.
-#}
    SELECT DISTINCT month_end_date AS reference_date
    FROM {{ ref('int_segmentation_person_month_spine') }}
{% endmacro %}


{% macro ltc_register_known_by(clinical_date_column, recorded_date_column, reference_date_column) %}
{#-
    An event counts at a reference date once both its clinical date and its
    recorded date are on or before that date.
-#}
    {{ clinical_date_column }} <= {{ reference_date_column }}
    AND (
        {{ recorded_date_column }} IS NULL
        OR CAST({{ recorded_date_column }} AS DATE) <= {{ reference_date_column }}
    )
{% endmacro %}
