{{ config(materialized='ephemeral') }}

{{ select_data_dictionary_history(
    'stg_ukhfd_data_dictionary_out_patient_attendance_outcome',
    excluded_columns=['category', 'notes', 'valid_to_date']
) }}

union all by name

{{ select_data_dictionary_history(
    'stg_ukhfd_data_dictionary_outcome_of_attendance',
    excluded_columns=['category', 'notes', 'valid_to_date']
) }}
