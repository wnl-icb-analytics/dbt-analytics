{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_mhsds_referral_closure_reason') }}

union all

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_csds_referral_closure_reason_legacy') }}
