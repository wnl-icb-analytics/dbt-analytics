{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_mhsds_referring_care_professional_staff_group') }}

union all

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_mhsds_referring_care_professional_staff_group_legacy') }}
