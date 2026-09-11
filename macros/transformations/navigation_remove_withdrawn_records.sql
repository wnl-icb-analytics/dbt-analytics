{% macro navigation_remove_withdrawn_records(sources) -%}
    {%- if is_incremental() -%}
        {# Compare narrow source keys so withdrawals do not wait for a full refresh. #}
        delete from {{ this }} as previous
        where not exists (
            select 1
            from (
                {% for record_type, model_name, key in sources %}
                select '{{ record_type }}' as source_record_type, {{ key }}::varchar as source_record_id
                from {{ ref(model_name) }}
                {% if not loop.last %}union all{% endif %}
                {% endfor %}
            ) as current_source
            where previous.source_record_type = current_source.source_record_type
                and previous.source_record_id = current_source.source_record_id
        )
    {%- endif -%}
{%- endmacro %}
