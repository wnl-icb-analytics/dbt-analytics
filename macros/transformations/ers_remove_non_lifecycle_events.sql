{% macro ers_remove_non_lifecycle_events() -%}
    {%- if is_incremental() -%}
        delete from {{ this }} as previous
        using {{ ref('fct_ers_referral_action') }} as current_source
        where previous.source_record_type = 'referral_action'
            and previous.source_record_id = current_source.action_id::varchar
            and not exists (
                select 1 from {{ ref('ers_healthcare_event_type') }} as mapping
                where current_source.action_code = mapping.action_code
            )
    {%- endif -%}
{%- endmacro %}
