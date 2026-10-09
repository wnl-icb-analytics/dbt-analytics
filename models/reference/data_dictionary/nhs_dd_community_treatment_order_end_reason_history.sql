{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_mhsds_community_treatment_order_end_reason') }}
