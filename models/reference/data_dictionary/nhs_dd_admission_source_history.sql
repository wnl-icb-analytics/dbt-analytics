{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_admission_source') }}

union all

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_source_of_admission_legacy') }}
