{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_mhsds_employment_status') }}

union all

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_mhsds_employment_status_general') }}
