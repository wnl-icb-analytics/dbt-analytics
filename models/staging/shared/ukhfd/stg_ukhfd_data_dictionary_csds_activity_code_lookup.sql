{% set code_sets = [
    ('procedure_scheme', 'raw_ukhfd_data_dictionary_procedure_scheme_in_use'),
    ('finding_scheme', 'raw_ukhfd_data_dictionary_mhsds_finding_scheme_in_use'),
    ('observation_scheme', 'raw_ukhfd_data_dictionary_mhsds_observation_scheme_in_use'),
    ('activity_type', 'raw_ukhfd_data_dictionary_csds_community_care_activity_type'),
    ('activity_type', 'raw_ukhfd_data_dictionary_csds_community_care_activity_type_legacy'),
    ('referral_closure_reason', 'raw_ukhfd_data_dictionary_mhsds_referral_closure_reason'),
    ('referral_closure_reason', 'raw_ukhfd_data_dictionary_csds_referral_closure_reason_legacy'),
    ('referral_rejection_reason', 'raw_ukhfd_data_dictionary_mhsds_referral_rejection_reason')
] %}

{% for code_set_name, raw_model_name in code_sets %}
select
    '{{ code_set_name }}' as code_set_name
    , definitions.source_code_set_name
    , definitions.code
    , definitions.description
    , definitions.short_description
    , definitions.category
    , definitions.notes
    , definitions.valid_from_date
    , definitions.valid_to_date
    , definitions.is_currently_valid
    , definitions.source_unique_key
    , definitions.source_imported_at
    , definitions.source_created_at
    , definitions.is_latest_definition
    , definitions.source_effective_from_at
    , definitions.source_effective_to_at
from (
    {{ select_ukhfd_data_dictionary_code_set(raw_model_name) }}
) as definitions
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
