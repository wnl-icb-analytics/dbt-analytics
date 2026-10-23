{% macro select_data_dictionary_history(staging_model_name, excluded_columns=[]) %}
select
    source_code_set_name,
    code,
    description,
    short_description,
    {% if 'category' not in excluded_columns %}category,{% endif %}
    {% if 'notes' not in excluded_columns %}notes,{% endif %}
    valid_from_date,
    {% if 'valid_to_date' not in excluded_columns %}valid_to_date,{% endif %}
    is_currently_valid,
    source_unique_key,
    source_imported_at,
    source_created_at,
    is_latest_definition,
    source_effective_from_at,
    source_effective_to_at
from {{ ref(staging_model_name) }}
{% endmacro %}
