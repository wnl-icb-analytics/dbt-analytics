{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_out_patient_attendance_outcome') }}

union all

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_outcome_of_attendance') }}
