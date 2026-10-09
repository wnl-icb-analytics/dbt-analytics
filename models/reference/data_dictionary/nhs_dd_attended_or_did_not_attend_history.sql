{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_attended_or_did_not_attend') }}
