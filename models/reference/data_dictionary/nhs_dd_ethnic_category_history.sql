{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history(
    'stg_ukhfd_data_dictionary_ethnic_category_code_2001',
    excluded_columns=['valid_to_date']
) }}

union all by name

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_ethnic_category_code') }}
