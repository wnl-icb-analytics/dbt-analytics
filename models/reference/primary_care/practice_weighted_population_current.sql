-- Practice need-weighted population for the latest loaded allocation base
-- year that has started. One row per practice. Never a future projection.
select
    practice_code,
    financial_year,
    financial_year_start,
    registered_patients,
    weighted_patients_core,
    weighted_to_registered_ratio,
    weighted_patients_general_acute,
    weighted_patients_community,
    weighted_patients_mental_health,
    weighted_patients_maternity,
    weighted_patients_prescribing,
    weighted_patients_primary_medical_care,
    weighted_patients_health_inequalities,
    source_file_version
from {{ ref('practice_weighted_population') }}
where is_current_base_year
