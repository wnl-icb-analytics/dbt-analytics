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
        and (
            {{ received_at }} is null
            or {{ received_at }} >= (
                select coalesce(max(source_received_at), '1900-01-01'::timestamp_ntz)
                from {{ this }}
                where source_record_type = '{{ source_record_type }}'
            )
        )
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
