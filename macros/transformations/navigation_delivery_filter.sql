{% macro navigation_delivery_filter(received_at, source_record_type) -%}
    {%- if is_incremental() -%}
        {# Replay the boundary delivery so equal timestamps cannot drop records. #}
        and (
            {{ received_at }} is null
            or {{ received_at }} >= (
                select coalesce(max(source_received_at), '1900-01-01'::timestamp_ntz)
                from {{ this }}
                where source_record_type = '{{ source_record_type }}'
            )
        )
    {%- endif -%}
{%- endmacro %}
