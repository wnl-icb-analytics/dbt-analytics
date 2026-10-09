{% set code_sets = [
    ('staff_group', 'raw_ukhfd_data_dictionary_mhsds_care_professional_staff_group'),
    ('main_specialty', 'stg_ukhfd_data_dictionary_main_specialty_code'),
    ('professional_registration_body', 'raw_ukhfd_data_dictionary_professional_registration_body_code'),
    ('job_role', 'raw_ukhfd_data_dictionary_care_professional_job_role_code')
] %}

{% for code_set_name, model_name in code_sets %}
select
    '{{ code_set_name }}' as code_set_name,
    definitions.*
from (
    {% if model_name.startswith('stg_') %}select * from {{ ref(model_name) }}{% else %}{{ select_ukhfd_data_dictionary_code_set(model_name) }}{% endif %}
) as definitions
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
