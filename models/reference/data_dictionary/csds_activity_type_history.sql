{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_csds_community_care_activity_type') }}

union all

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_csds_community_care_activity_type_legacy') }}
