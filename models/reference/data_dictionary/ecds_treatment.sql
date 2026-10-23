{{ select_ecds_code_set(
    'TREATMENT',
    extra_columns=[
        'pbr_category',
        'cds_code_mapping_used_for_hrg_grouping',
        'cds_treatment_mapping_used_for_hrg_grouping',
        'notes'
    ],
    has_valid_to_date=true
) }}
