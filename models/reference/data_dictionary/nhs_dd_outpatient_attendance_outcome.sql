{{ select_latest_data_dictionary_definitions(
    'nhs_dd_outpatient_attendance_outcome_history',
    preferred_code_set_name='Out_Patient_Attendance_Outcome',
    excluded_columns=['category', 'notes', 'valid_to_date']
) }}
