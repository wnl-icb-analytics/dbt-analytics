{% macro select_data_dictionary_history(staging_model_name) %}
select
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
from {{ ref(staging_model_name) }}
{% endmacro %}
