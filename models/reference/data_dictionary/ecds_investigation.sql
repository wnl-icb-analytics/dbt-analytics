{{ select_ecds_code_set(
    'INVESTIGATION',
    extra_columns=[
        'pbr_category',
        'cds_code_mapping_used_for_hrg_grouping',
        'cds_investigation_mapping_used_for_hrg_grouping'
    ],
    has_valid_to_date=true
) }}
