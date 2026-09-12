{# Retired codes keep their UKHFD definitions so older IAPT submissions stay labelled. #}
{% set code_sets = [
    ('source_of_referral', 'raw_ukhfd_data_dictionary_iapt_source_of_referral'),
    ('discharge_reason', 'raw_ukhfd_data_dictionary_iapt_discharge_reason'),
    ('discharge_reason_legacy', 'raw_ukhfd_data_dictionary_iapt_discharge_reason_legacy'),
    ('previous_diagnosed_condition_indicator', 'raw_ukhfd_data_dictionary_iapt_previous_diagnosed_condition_indicator'),
    ('onward_referral_reason', 'raw_ukhfd_data_dictionary_iapt_onward_referral_reason'),
    ('appointment_type', 'raw_ukhfd_data_dictionary_iapt_appointment_type'),
    ('psychotropic_medication_usage', 'raw_ukhfd_data_dictionary_iapt_psychotropic_medication_usage'),
    ('short_notice_cancellation_indicator', 'raw_ukhfd_data_dictionary_iapt_short_notice_cancellation_indicator'),
    ('integrated_ltc_service_indicator', 'raw_ukhfd_data_dictionary_iapt_integrated_ltc_service_indicator'),
    ('coding_significance', 'raw_ukhfd_data_dictionary_iapt_coding_significance')
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
