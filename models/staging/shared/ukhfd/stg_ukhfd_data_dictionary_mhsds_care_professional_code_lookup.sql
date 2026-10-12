{% set code_sets = [
    ('staff_group', 'raw_ukhfd_data_dictionary_mhsds_care_professional_staff_group'),
    ('main_specialty', 'stg_ukhfd_data_dictionary_main_specialty_code'),
    ('professional_registration_body', 'raw_ukhfd_data_dictionary_professional_registration_body_code'),
    ('job_role', 'raw_ukhfd_data_dictionary_care_professional_job_role_code')
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
