{% macro navigation_delivery_filter(received_at, source_record_type=none, source_record_type_column=none) -%}
    {%- if is_incremental() -%}
        {# Replay the boundary delivery so equal timestamps cannot drop records. #}
        {%- if source_record_type_column -%}
        {# Correlated form: one input carrying several record types uses each type's own watermark. #}
        and (
            {{ received_at }} is null
            or {{ received_at }} >= coalesce((
                select max(watermark.source_received_at)
                from {{ this }} as watermark
                where watermark.source_record_type = {{ source_record_type_column }}
            ), '1900-01-01'::timestamp_ntz)
        )
        {%- else -%}
        {%- set watermark = '1900-01-01 00:00:00.000000000' -%}
        {%- if execute -%}
            {%- set result = run_query(
                "select to_varchar(coalesce(max(source_received_at), '1900-01-01'::timestamp_ntz), 'YYYY-MM-DD HH24:MI:SS.FF9') from "
                ~ this ~ " where source_record_type = '" ~ source_record_type ~ "'"
            ) -%}
            {%- set watermark = result.columns[0].values()[0] -%}
        {%- endif -%}
        and ({{ received_at }} is null or {{ received_at }} >= '{{ watermark }}'::timestamp_ntz)
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
