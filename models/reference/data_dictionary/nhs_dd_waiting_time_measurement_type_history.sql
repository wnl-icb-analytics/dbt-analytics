{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_mhsds_waiting_time_measurement_type') }}
