{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_main_specialty_code') }}
