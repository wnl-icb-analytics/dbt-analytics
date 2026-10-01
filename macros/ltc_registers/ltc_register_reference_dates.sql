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
    Month-ends evaluated by the monthly register history: the last 60 completed
    month-ends. Bounded here as well as in the person-month spine, which keeps
    aged-out months after an incremental run.
-#}
    SELECT DISTINCT month_end_date AS reference_date
    FROM {{ ref('int_segmentation_person_month_spine') }}
    WHERE month_end_date BETWEEN LAST_DAY(DATEADD('month', -60, CURRENT_DATE()))
        AND LAST_DAY(DATEADD('month', -1, CURRENT_DATE()))
{% endmacro %}


{% macro ltc_register_known_by(clinical_date_column, recorded_date_column, reference_date_column) %}
{#-
    An event counts at a reference date once both its clinical (or order) date
    and its recorded date are on or before that date. A null recorded date does
    not block the event.
-#}
    CAST({{ clinical_date_column }} AS DATE) <= {{ reference_date_column }}
    AND (
        {{ recorded_date_column }} IS NULL
        OR CAST({{ recorded_date_column }} AS DATE) <= {{ reference_date_column }}
    )
{% endmacro %}


{% macro ltc_relation_built_on(relation) %}
{#-
    Day a table was last built (its created_on, as dbt replaces tables on each
    build), as 'YYYY-MM-DD' in the session time zone, matching CURRENT_DATE() in the
    build. Read with SHOW TABLES when the caller compiles; none if the table cannot
    be found or the project is only being parsed.
-#}
{%- set built_on = none -%}
{%- if execute -%}
    {%- do run_query("SHOW TABLES LIKE '" ~ (relation.identifier | upper) ~ "' IN SCHEMA " ~ relation.database ~ "." ~ relation.schema) -%}
    {%- set result = run_query("SELECT TO_VARCHAR(MAX(\"created_on\")::DATE, 'YYYY-MM-DD') FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE UPPER(\"name\") = '" ~ (relation.identifier | upper) ~ "'") -%}
    {%- if result.rows | length > 0 -%}
        {%- set built_on = result.rows[0][0] -%}
    {%- endif -%}
{%- endif -%}
{{ return(built_on) }}
{% endmacro %}


{% macro ltc_live_register_as_of(live_relation, age_built_on, cache) %}
{#-
    Date a live register's rules were evaluated, as a date literal: the earlier of
    the day it was built and the day dim_person_age (its source of age) was built
    (age_built_on, from ltc_relation_built_on). Live facts use CURRENT_DATE() and
    today's age at build time, so comparing them with the as-of macros must use this
    date, not the date the comparison runs. Results are kept in cache (a dict) so a
    caller can ask again without another lookup. Falls back to CURRENT_DATE().
-#}
{%- set key = live_relation | string -%}
{%- if key in cache -%}
    {{ return(cache[key]) }}
{%- endif -%}
{%- set candidates = [] -%}
{%- set live_built_on = ltc_relation_built_on(live_relation) -%}
{%- if live_built_on -%}{%- do candidates.append(live_built_on) -%}{%- endif -%}
{%- if age_built_on -%}{%- do candidates.append(age_built_on) -%}{%- endif -%}
{%- set as_of = ("'" ~ (candidates | min) ~ "'::DATE") if candidates | length > 0 else 'CURRENT_DATE()' -%}
{%- do cache.update({key: as_of}) -%}
{{ return(as_of) }}
{% endmacro %}
