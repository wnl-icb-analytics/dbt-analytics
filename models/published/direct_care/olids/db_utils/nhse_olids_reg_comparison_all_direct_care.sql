{{
    config(
        materialized='table',
        alias='nhse_olids_reg_comparison_all',
        tags=['data_quality', 'published', 'direct_care']
    )
}}

select
    snapshot_date,
    practice_code,
    practice_name,
    borough,
    nhse_registered_patients,
    olids_registered_patients,
    difference,
    absolute_difference,
    percent_difference,
    absolute_percent_difference,
    meets_acceptance_criteria,
    variance_category,
    is_latest_snapshot
from {{ ref('int_nhse_olids_practice_registration_comparison') }}
