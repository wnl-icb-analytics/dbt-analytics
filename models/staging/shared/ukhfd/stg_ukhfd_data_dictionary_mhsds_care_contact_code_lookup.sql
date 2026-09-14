{% set code_sets = [
    ('administrative_category', 'raw_ukhfd_data_dictionary_mhsds_administrative_category'),
    ('care_contact_cancellation_reason', 'raw_ukhfd_data_dictionary_mhsds_care_contact_cancellation_reason'),
    ('patient_therapy_mode', 'raw_ukhfd_data_dictionary_mhsds_care_contact_patient_therapy_mode'),
    ('care_contact_subject', 'raw_ukhfd_data_dictionary_mhsds_care_contact_subject'),
    ('consultation_medium_used', 'raw_ukhfd_data_dictionary_mhsds_consultation_medium_used'),
    ('consultation_type', 'raw_ukhfd_data_dictionary_mhsds_consultation_type'),
    ('group_therapy_indicator', 'raw_ukhfd_data_dictionary_mhsds_group_therapy_indicator'),
    ('interpreter_present_indicator', 'raw_ukhfd_data_dictionary_mhsds_interpreter_present_indicator'),
    ('perinatal_partner_assessment_offer_indicator', 'raw_ukhfd_data_dictionary_mhsds_perinatal_partner_assessment_offer_indicator'),
    ('place_of_safety_indicator', 'raw_ukhfd_data_dictionary_mhsds_place_of_safety_indicator'),
    ('planned_care_contact_indicator', 'raw_ukhfd_data_dictionary_mhsds_planned_care_contact_indicator'),
    ('reason_patient_no_imca', 'raw_ukhfd_data_dictionary_mhsds_reason_patient_no_imca'),
    ('reason_patient_no_imha', 'raw_ukhfd_data_dictionary_mhsds_reason_patient_no_imha'),
    ('reasonable_adjustment_made_indicator', 'raw_ukhfd_data_dictionary_mhsds_reasonable_adjustment_made_indicator')
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
