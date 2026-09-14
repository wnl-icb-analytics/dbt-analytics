{% set code_sets = [
    ('admission_source', 'raw_ukhfd_data_dictionary_admission_source'),
    ('admission_source', 'raw_ukhfd_data_dictionary_source_of_admission_legacy'),
    ('admission_method', 'raw_ukhfd_data_dictionary_admission_method'),
    ('planned_discharge_destination', 'raw_ukhfd_data_dictionary_planned_destination_of_discharge'),
    ('planned_discharge_destination', 'raw_ukhfd_data_dictionary_discharge_destination_legacy'),
    ('discharge_method', 'raw_ukhfd_data_dictionary_discharge_method'),
    ('discharge_destination', 'raw_ukhfd_data_dictionary_destination_of_discharge'),
    ('discharge_destination', 'raw_ukhfd_data_dictionary_discharge_destination_legacy'),
    ('ward_intended_sex', 'raw_ukhfd_data_dictionary_ward_intended_sex_of_patients'),
    ('ward_clinical_care_intensity', 'raw_ukhfd_data_dictionary_ward_intended_clinical_care_intensity'),
    ('ward_intended_age_group', 'raw_ukhfd_data_dictionary_ward_intended_age_group_for_mental_health'),
    ('ward_setting', 'raw_ukhfd_data_dictionary_ward_setting_type_for_mental_health'),
    ('ward_security_level', 'raw_ukhfd_data_dictionary_ward_security_level'),
    ('locked_ward_indicator', 'raw_ukhfd_data_dictionary_locked_ward_indicator')
] %}

{% for code_set_name, raw_model_name in code_sets %}
select
    '{{ code_set_name }}' as code_set_name,
    definitions.*
from (
    {{ select_ukhfd_data_dictionary_code_set(raw_model_name) }}
) as definitions
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
