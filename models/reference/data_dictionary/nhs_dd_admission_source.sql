{{ select_latest_data_dictionary_definitions(
    'nhs_dd_admission_source_history',
    preferred_code_set_name='Admission_Source',
    excluded_columns=['category', 'valid_to_date']
) }}
