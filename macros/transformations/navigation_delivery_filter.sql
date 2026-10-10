{% macro navigation_delivery_filter(received_at, source_record_type) -%}
    {%- if is_incremental() -%}
        {%- set watermark = '1900-01-01 00:00:00.000000000' -%}
        {%- if execute -%}
            {%- set result = run_query(
                "select to_varchar(coalesce(max(source_received_at), '1900-01-01'::timestamp_ntz), 'YYYY-MM-DD HH24:MI:SS.FF9') from "
                ~ this ~ " where source_record_type = '" ~ source_record_type ~ "'"
            ) -%}
            {%- set watermark = result.columns[0].values()[0] -%}
        {%- endif -%}
        {# Replay the boundary delivery so equal timestamps cannot drop records. #}
        and ({{ received_at }} is null or {{ received_at }} >= '{{ watermark }}'::timestamp_ntz)
    {%- endif -%}
{%- endmacro %}
