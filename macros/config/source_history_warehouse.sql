{% macro source_history_warehouse(restore=false) -%}
    {# Evaluate in the hook so partial parsing cannot cache full-refresh behaviour. #}
    {%- if execute and target.role | upper == 'DBT_ADMIN' -%}
        {%- if restore -%}
            use warehouse {{ adapter.quote(target.warehouse) }}
        {%- elif not is_incremental() -%}
            use warehouse {{ adapter.quote('WH_WNL_OLIDS_L') }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
