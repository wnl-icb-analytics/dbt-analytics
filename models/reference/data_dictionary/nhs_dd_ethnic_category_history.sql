{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_ethnic_category_code_2001') }}

union all

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_ethnic_category_code') }}
