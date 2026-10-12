{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history(
    'stg_ukhfd_data_dictionary_admission_source',
    excluded_columns=['category', 'valid_to_date']
) }}

union all by name

{{ select_data_dictionary_history(
    'stg_ukhfd_data_dictionary_source_of_admission_legacy',
    excluded_columns=['valid_to_date']
) }}
