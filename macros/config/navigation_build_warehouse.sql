{% macro navigation_build_warehouse(restore=false) -%}
    {# Evaluate in a hook: FULL_REFRESH must not be cached in parse-time config. #}
    {%- if execute and target.role | upper == 'DBT_ADMIN' -%}
        {%- if restore -%}
            use warehouse {{ adapter.quote(target.warehouse) }}
        {%- elif not is_incremental() -%}
            use warehouse {{ adapter.quote('WH_WNL_OLIDS_L') }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
