{% set code_sets = [
    ('priority_type', 'raw_ukhfd_data_dictionary_priority_type'),
    ('reason_for_referral', 'raw_ukhfd_data_dictionary_csds_reason_for_referral'),
    ('referring_staff_group', 'raw_ukhfd_data_dictionary_csds_referring_staff_group')
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
from ({{ select_ukhfd_data_dictionary_code_set(raw_model_name) }}) as definitions
{% if not loop.last %}union all{% endif %}
{% endfor %}
