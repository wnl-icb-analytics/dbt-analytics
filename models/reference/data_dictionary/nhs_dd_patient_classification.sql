{{ select_latest_data_dictionary_definitions(
    'nhs_dd_patient_classification_history',
    excluded_columns=['category', 'valid_to_date']
) }}
