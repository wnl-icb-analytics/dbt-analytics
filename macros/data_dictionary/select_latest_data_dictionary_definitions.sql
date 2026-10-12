{#
    One row per code from a data dictionary history model.
    preferred_code_set_name: for history models that combine a current and a
    legacy code set, the code set whose definition wins where both hold a code.
    excluded_columns: optional fields that this code list does not populate.
#}
{% macro select_latest_data_dictionary_definitions(history_model_name, preferred_code_set_name=none, excluded_columns=[]) %}
select
    code,
    description,
    short_description,
    {% if 'category' not in excluded_columns %}category,{% endif %}
    {% if 'notes' not in excluded_columns %}notes,{% endif %}
    valid_from_date,
    {% if 'valid_to_date' not in excluded_columns %}valid_to_date,{% endif %}
    is_currently_valid,
    source_code_set_name,
    source_imported_at,
    source_effective_from_at as definition_updated_at
from {{ ref(history_model_name) }}
where is_latest_definition
{%- if preferred_code_set_name %}
qualify row_number() over (
    partition by code
    order by
        source_code_set_name = '{{ preferred_code_set_name }}' desc,
        source_effective_from_at desc nulls last,
        source_imported_at desc nulls last,
        source_unique_key desc
) = 1
{%- endif %}
{% endmacro %}
