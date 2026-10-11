{% set code_sets = [
    ('priority_type', 'stg_ukhfd_data_dictionary_priority_type'),
    ('reason_for_referral', 'raw_ukhfd_data_dictionary_csds_reason_for_referral'),
    ('referring_staff_group', 'raw_ukhfd_data_dictionary_csds_referring_staff_group'),
    ('waiting_time_measurement_type', 'raw_ukhfd_data_dictionary_csds_waiting_time_measurement_type')
] %}

with combined as (
{% for code_set_name, model_name in code_sets %}
select
    '{{ code_set_name }}' as code_set_name,
    definitions.*
from (
    {% if model_name.startswith('stg_') %}select * from {{ ref(model_name) }}{% else %}{{ select_ukhfd_data_dictionary_code_set(model_name) }}{% endif %}
) as definitions
{% if not loop.last %}union all by name{% endif %}
{% endfor %}
)

select
    code_set_name,
    source_code_set_name,
    code,
    description,
    short_description,
    category,
    notes,
    valid_from_date,
    valid_to_date,
    is_currently_valid,
    source_unique_key,
    source_imported_at,
    source_created_at,
    is_latest_definition,
    source_effective_from_at,
    source_effective_to_at
from combined
