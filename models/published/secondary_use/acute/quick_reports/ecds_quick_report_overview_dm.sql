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
is_last_12m,
month_start_date,
month_label, 
month_sort, 
week_start_date,
week_label,
week_end_date,
comm_group, 
is_wnl_registered, 
CASE 
WHEN practice_code in ('E84023','E84624','E84667') THEN 'West and North London'
WHEN icb_name IS NULL THEN 'Unknown'
ELSE icb_name END AS icb_name,
NVL(health_borough,'Not WNL') as health_borough,
practice_neighbourhood,
pcn_name,
practice_code,
CASE 
WHEN practice_name = 'Hesa Medical Centre - Y00352' THEN 'HESA Medical Centre'
ELSE practice_name END AS practice_name,
is_wnl_resident, 
NVL(residential_location,'Unknown') as residence_group, 
age_band_nhs,  
age_sort,
provider_shorthand, 
site_name,  
dept_type_desc, 
arrival_mode_desc, 
arrival_by_ambulance,
admission_status, 
CASE
WHEN admission_status = 'Admitted' THEN TRUE ELSE FALSE END AS is_admitted,
duration IS NULL AS is_duration_missing,
SUM(attendance_count) as total_attendance_count,
SUM(IFF(is_4_hour_breach,  attendance_count, 0)) AS breach_4hr,
SUM(IFF(is_12_hour_breach, attendance_count, 0)) AS breach_12hr,
SUM(IFF(is_72_hour_breach, attendance_count, 0)) AS breach_72hr,
SUM(IFF(duration IS NULL, attendance_count, 0)) AS missing_duration
FROM {{ ref("ecds_quick_report_base") }}
--FROM DEV__MODELLING.ACUTE.INT_ECDS_QUICK_REPORT_BASE
group by all