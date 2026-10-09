{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_mhsds_post_incident_review_not_held_reason') }}
