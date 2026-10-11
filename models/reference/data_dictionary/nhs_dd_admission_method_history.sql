{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history(
    'stg_ukhfd_data_dictionary_admission_method',
    excluded_columns=['valid_to_date']
) }}
