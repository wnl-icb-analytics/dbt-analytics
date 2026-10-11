{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_nhs_dd_consultation_medium_used') }}
