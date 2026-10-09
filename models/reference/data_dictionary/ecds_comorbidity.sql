{{ select_ecds_code_set(
    'COMORBIDITY',
    extra_columns=[
        'comorbidity_icd10_codes',
        'is_charlson_comorbidity',
        'is_elixhauser_comorbidity',
        'notes'
    ],
    has_valid_to_date=true
) }}
