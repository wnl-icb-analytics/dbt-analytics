{% macro get_latest_events(from_table, partition_by='person_id', order_by='clinical_effective_date', direction='DESC') %}
    {#- Get latest events for each person from a set of events -#}
    {#- Uses Snowflake's QUALIFY statement for efficient filtering of window functions -#}

    {%- if partition_by is string -%}
        {%- set partition_cols = partition_by -%}
    {%- elif partition_by is iterable -%}
        {%- set partition_cols = partition_by | join(', ') -%}
    {%- else -%}
        {%- set partition_cols = partition_by -%}
    {%- endif -%}

    {%- set order_items = [order_by] if order_by is string else order_by -%}
    {%- set order_terms = [] -%}
    {%- for item in order_items -%}
        {%- set term = item | string | trim -%}
        {%- set upper_term = term | upper -%}
        {%- set nulls_clause = '' -%}
        {%- if upper_term.endswith(' NULLS FIRST') -%}
            {%- set nulls_clause = term[-12:] -%}
            {%- set term = term[:-12] | trim -%}
        {%- elif upper_term.endswith(' NULLS LAST') -%}
            {%- set nulls_clause = term[-11:] -%}
            {%- set term = term[:-11] | trim -%}
        {%- endif -%}
        {%- set upper_term = term | upper -%}
        {%- if not (upper_term.endswith(' ASC') or upper_term.endswith(' DESC')) -%}
            {%- set term = term ~ ' ' ~ direction -%}
        {%- endif -%}
        {%- do order_terms.append(term ~ nulls_clause) -%}
    {%- endfor -%}

SELECT *
FROM {{ from_table }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY {{ partition_cols }}
    ORDER BY {{ order_terms | join(', ') }}
) = 1
{% endmacro %}
