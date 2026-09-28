{% set code_sets = [
    ('finding_scheme', 'raw_ukhfd_data_dictionary_mhsds_finding_scheme_in_use'),
    ('observation_scheme', 'raw_ukhfd_data_dictionary_mhsds_observation_scheme_in_use')
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
