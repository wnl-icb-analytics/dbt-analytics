{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history(
    'stg_ukhfd_data_dictionary_mhsds_person_stated_gender',
    excluded_columns=['category', 'notes', 'valid_to_date']
) }}
