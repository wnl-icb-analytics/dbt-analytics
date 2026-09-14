{% macro select_ukhfd_data_dictionary_code_set(raw_model_name) %}
select
    nullif(trim(attr_name), '') as source_code_set_name,
    upper(nullif(trim(main_code_text), '')) as code,
    nullif(trim(main_description), '') as description,
    nullif(trim(main_description_60_chars), '') as short_description,
    nullif(trim(category), '') as category,
    nullif(trim(notes), '') as notes,
    cast(valid_from as date) as valid_from_date,
    cast(valid_to as date) as valid_to_date,
    in_source_table = 1 as is_currently_valid,
    nullif(trim(unique_column), '') as source_unique_key,
    import_date as source_imported_at,
    created_date as source_created_at,
    is_latest = 1 as is_latest_definition,
    effective_from as source_effective_from_at,
    effective_to as source_effective_to_at
from {{ ref(raw_model_name) }}
{% endmacro %}
