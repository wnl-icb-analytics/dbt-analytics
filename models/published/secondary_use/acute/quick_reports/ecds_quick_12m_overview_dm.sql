{{
    config(
        materialized='view',
        tags=['quick_reports'])
}}
/*FINAL MODEL Aggregate table for OVERVIEW describing AE Encounters as rolling 12m from INT_ECDS_QUICK_12M_ROLLING_BASE table. Commisionned by WNL ~1 million rows.
Data Quality flag if duration is missing (2.5k)
*/ 
SELECT 
financial_year_label, 
attendance_month_label, 
attendance_month_sort, 
comm_group, 
is_wnl_registered, 
health_borough,
practice_neighbourhood,
pcn_name,
practice_code,
practice_name,
is_wnl_resident, 
residential_location as residence_group, 
age_band_nhs,  
provider_shorthand, 
site_name,  
dept_type_desc, 
arrival_mode_desc, 
admission_status, 
duration IS NULL AS is_duration_missing,
SUM(attendance_count) as total_attendance_count,
SUM(IFF(is_4_hour_breach,  attendance_count, 0)) AS attendances_4hr_breach,
SUM(IFF(is_12_hour_breach, attendance_count, 0)) AS attendances_12hr_breach,
SUM(IFF(is_72_hour_breach, attendance_count, 0)) AS attendances_72hr_breach,
SUM(IFF(duration IS NULL, attendance_count, 0)) AS attendances_missing_duration
FROM {{ ref("int_ecds_quick_12m_rolling_base") }}
--FROM DEV__MODELLING.ACUTE.INT_ECDS_QUICK_12M_ROLLING_BASE
group by all