{{ select_ecds_code_set(
    'DIAGNOSIS',
    extra_columns=[
        'ecds_group2',
        'ecds_group3',
        'ecds_search_terms',
        'icd10_mapping',
        'icd10_description',
        'icd11_mapping',
        'icd11_description',
        'is_valid_for_male',
        'is_valid_for_female',
        'is_injury',
        'is_allergy',
        'is_notifiable_disease',
        'is_same_day_emergency_care',
        'is_ads',
        'notes'
    ],
    has_valid_to_date=true
) }}
