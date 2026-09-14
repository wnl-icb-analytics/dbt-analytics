-- Practice need-weighted populations by allocation base year, from the NHS
-- England revenue allocations practice-level workbooks via UKHFD. One row per
-- practice and base year. All values are modelled for the base year, not
-- actual list counts; base years after today are projections. Practice-years
-- without a positive registered population are excluded. The headline
-- is NHS England's Core Services weighted population; the service-specific
-- populations are carried as separate columns.
with metrics as (
    select
        practice_code,
        financial_year_start,
        source_file_version,
        metric_name,
        metric_value
    from {{ ref('stg_ukhfd_weighted_regs_by_gp_practice_base') }}
    where metric_name in (
        'Registered population (base year)',
        'Core Services weighted population',
        'General and Acute (G&A) weighted population',
        'Community Services (CS) weighted population',
        'Mental Health (MH) weighted population',
        'Maternity weighted population',
        'Prescribing weighted population',
        'Primary Medical Care weighted population',
        'Health inequalities (HI) weighted population'
    )
),

practice_year as (
    select
        practice_code,
        financial_year_start,
        max(source_file_version) as source_file_version,
        max(iff(metric_name = 'Registered population (base year)', metric_value, null))
            as registered_patients,
        max(iff(metric_name = 'Core Services weighted population', metric_value, null))
            as weighted_patients_core,
        max(iff(metric_name = 'General and Acute (G&A) weighted population', metric_value, null))
            as weighted_patients_general_acute,
        max(iff(metric_name = 'Community Services (CS) weighted population', metric_value, null))
            as weighted_patients_community,
        max(iff(metric_name = 'Mental Health (MH) weighted population', metric_value, null))
            as weighted_patients_mental_health,
        max(iff(metric_name = 'Maternity weighted population', metric_value, null))
            as weighted_patients_maternity,
        max(iff(metric_name = 'Prescribing weighted population', metric_value, null))
            as weighted_patients_prescribing,
        max(iff(metric_name = 'Primary Medical Care weighted population', metric_value, null))
            as weighted_patients_primary_medical_care,
        max(iff(metric_name = 'Health inequalities (HI) weighted population', metric_value, null))
            as weighted_patients_health_inequalities
    from metrics
    group by practice_code, financial_year_start
),

flagged as (
    select
        *,
        financial_year_start > current_date() as is_projection,
        coalesce(
            financial_year_start = max(iff(financial_year_start <= current_date(), financial_year_start, null))
                over (partition by practice_code),
            false
        ) as is_current_base_year
    from practice_year
    where registered_patients > 0
)

select
    practice_code,
    concat(
        year(financial_year_start),
        '/',
        right((year(financial_year_start) + 1)::varchar, 2)
    ) as financial_year,
    financial_year_start,
    is_current_base_year,
    is_projection,
    registered_patients,
    weighted_patients_core,
    div0(weighted_patients_core, registered_patients) as weighted_to_registered_ratio,
    weighted_patients_general_acute,
    weighted_patients_community,
    weighted_patients_mental_health,
    weighted_patients_maternity,
    weighted_patients_prescribing,
    weighted_patients_primary_medical_care,
    weighted_patients_health_inequalities,
    source_file_version
from flagged
