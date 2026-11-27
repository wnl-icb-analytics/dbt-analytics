{% macro select_ukhfd_data_dictionary_code_set(raw_model_name, excluded_columns=[]) %}
{#- Trim tabs and line breaks as well as spaces; UKHFD text can carry them. -#}
select
    nullif(trim(attr_name, ' ' || char(9) || char(10) || char(13)), '') as source_code_set_name,
    upper(nullif(trim(main_code_text, ' ' || char(9) || char(10) || char(13)), '')) as code,
    nullif(trim(main_description, ' ' || char(9) || char(10) || char(13)), '') as description,
    nullif(trim(main_description_60_chars, ' ' || char(9) || char(10) || char(13)), '') as short_description,
    {% if 'category' not in excluded_columns %}nullif(trim(category, ' ' || char(9) || char(10) || char(13)), '') as category,{% endif %}
    {% if 'notes' not in excluded_columns %}nullif(trim(notes, ' ' || char(9) || char(10) || char(13)), '') as notes,{% endif %}
    cast(valid_from as date) as valid_from_date,
    {% if 'valid_to_date' not in excluded_columns %}cast(valid_to as date) as valid_to_date,{% endif %}
    in_source_table = 1 as is_currently_valid,
    nullif(trim(unique_column, ' ' || char(9) || char(10) || char(13)), '') as source_unique_key,
    import_date as source_imported_at,
    created_date as source_created_at,
    is_latest = 1 as is_latest_definition,
    effective_from as source_effective_from_at,
    effective_to as source_effective_to_at
from {{ ref(raw_model_name) }}
{% endmacro %}
