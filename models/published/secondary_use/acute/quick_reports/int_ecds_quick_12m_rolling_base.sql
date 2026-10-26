{{
    config(
        materialized='table',
        tags=['quick_reports'])
}}
/* Base table AE Encounters for rolling 12m view from OBT_ENCOUNTER_UEC reporting table.
impute missing demographics from PDS Demographics
keep to WNL COMM GROUP for now
where comm_group <> 'WNL'2.3 million and where comm_group = 'WNL' 1.9 million
*/

WITH WNL_NAMES AS (
SELECT DISTINCT
        provider_code,
        provider_shorthand,
        provider_name,
FROM {{ ref('stg_reference_wnl_reporting_names') }}
--FROM STAGING.REFERENCE.STG_REFERENCE_WNL_REPORTING_NAMES
where provider_type = 'Acute'
)
--get accurate residential location.
,borough_res_category as (
SELECT *
,CASE
    WHEN lad25_cd IN ('E09000003', 'E09000007', 'E09000010', 'E09000014', 'E09000019','E09000005','E09000009','E09000013','E09000015','E09000017','E09000018','E09000020','E09000033') then 'WNL'
    WHEN lad25_cd NOT IN ('E09000003', 'E09000007', 'E09000010', 'E09000014', 'E09000019','E09000005','E09000009','E09000013','E09000015','E09000017','E09000018','E09000020','E09000033')
    AND lad25_cd LIKE 'E09%' THEN 'Other London' 
    WHEN lad25_cd IS NULL THEN 'Unknown' 
    ELSE 'Outside London' END AS residential_location
FROM {{ ref('stg_reference_lsoa21_ward25_lad25') }}
--FROM STAGING.REFERENCE.STG_REFERENCE_LSOA21_WARD25_LAD25
)
--build main table for rolling 12m view
,base_table_rolling_12m AS (
SELECT
--commissioning details
u.sk_patient_id,
u.commissioner_code_at_event as commissioner_code,
IFF(u.commissioner_code_at_event IN ('07X','07M','08H','08D','07R','93C','07P','07W','07Y','08C','08E','08G','08Y','09A','W2U3Z','Y05','T04','Z9B2Z'),'WNL','Other') comm_group,
u.reg_practice_at_event as practice_code,
u.reg_practice_name_latest as practice_name,
u.reg_practice_pcn_name_latest as pcn_name,
u.reg_practice_neighbourhood_name_latest as practice_neighbourhood,
u.reg_practice_health_borough_name_latest as health_borough,
CASE WHEN u.reg_practice_health_borough_name_latest is null THEN FALSE ELSE TRUE END AS is_wnl_registered,
--provider
u.organisation_id as provider_code,
wnl.provider_shorthand,
wnl.provider_name,
u.site_id,
u.site_name,
u.pod as dept_code,
CASE 
WHEN u.department_type = '01' THEN 'Major'
WHEN u.department_type = '02' THEN 'Mono'
WHEN u.department_type = '03' THEN 'UTC'
WHEN department_type = '04' THEN 'WIC'
WHEN department_type = '05' THEN 'SDEC'
ELSE department_type END as dept_type_desc,
--attendance event
u.start_date as attendance_date,
u.duration,
CASE 
        WHEN MONTH(u.start_date) >= 4 
        THEN YEAR(u.start_date) || '/' || RIGHT(YEAR(u.start_date) + 1, 2)
        ELSE (YEAR(u.start_date) - 1) || '/' || RIGHT(YEAR(u.start_date), 2)
    END as financial_year_label,
TO_NUMBER(TO_CHAR(u.start_date, 'YYYYMM')) AS attendance_month_sort,
MONTHNAME(u.start_date) || '-' || YEAR(u.start_date) AS attendance_month_label,
--duration > 240 mins
u.is_four_hour_breach as is_4_hour_breach,
--duration > 720 mins
IFF(
    u.duration IS NULL,
    NULL,
    u.over_12_hours_count = 1
) AS is_12_hour_breach,
--duration > 4320 mins
IFF(
    u.duration IS NULL,
    NULL,
    u.over_72_hours_count = 1
) AS is_72_hour_breach,
u.arrival_mode_desc,
u.hrg_code,
u.core_hrg_desc,
u.chief_complaint_desc,
u.chief_complaint_ecds_group1,
u.primary_diagnosis_desc_ecds_group1,
u.is_mental_health_related_attendance,
u.acuity_desc,
discharge_destination_group as admission_status,
--person_demographics
CASE WHEN g.lad25_nm IS NOT NULL AND g.residential_location = 'WNL' THEN TRUE ELSE FALSE END AS is_wnl_resident,
g.lad25_nm as borough_resident,
g.residential_location,
-- 5-year age bands
-- CASE
--     WHEN u.age_at_event < 0 THEN 'Unknown'
--     WHEN u.age_at_event >= 85 THEN '85+'
--     ELSE TO_VARCHAR(FLOOR(u.age_at_event / 5) * 5) || '-' || TO_VARCHAR(FLOOR(u.age_at_event / 5) * 5 + 4)
-- END AS age_band_5y,
-- NHS Digital age bands (used in Health Survey for England)
CASE
    WHEN u.age_at_event < 0 THEN 'Unknown'
    WHEN u.age_at_event < 5 THEN '0-4'
    WHEN u.age_at_event < 15 THEN '5-14'
    WHEN u.age_at_event < 25 THEN '15-24'
    WHEN u.age_at_event < 35 THEN '25-34'
    WHEN u.age_at_event < 45 THEN '35-44'
    WHEN u.age_at_event < 55 THEN '45-54'
    WHEN u.age_at_event < 65 THEN '55-64'
    WHEN u.age_at_event < 75 THEN '65-74'
    WHEN u.age_at_event < 85 THEN '75-84'
    ELSE '85+'
END AS age_band_nhs,
COALESCE(u.gender_desc_at_event,pds.gender) as gender,
COALESCE(u.ethnicity_at_event,pds.ethnicity_code) as ethnicity_code,
COALESCE(u.ethnicity_desc_at_event,pds.ethnicity_desc) as ethnicity,
COALESCE(u.deprivation_decile_at_event,pds.residence_imd_decile) as imd_decile_25,
COALESCE(u.lsoa_21_at_event,pds.lsoa_21) as lsoa_21,
u.attendance_count
FROM {{ ref("obt_encounter_uec") }} u
--FROM REPORTING.ACUTE.OBT_ENCOUNTER_UEC u
INNER JOIN WNL_NAMES wnl on u.organisation_id = wnl.provider_code
LEFT JOIN {{ ref("int_person_pds_demographics") }} pds using (sk_patient_id)
--LEFT JOIN MODELLING.POPULATION.INT_PERSON_PDS_DEMOGRAPHICS pds using (sk_patient_id)
LEFT JOIN borough_res_category g on COALESCE(u.lsoa_21_at_event,pds.lsoa_21) = g.lsoa21_cd
where u.start_date >= DATEADD('month', -12, CURRENT_DATE)
)
select *
from base_table_rolling_12m
where comm_group = 'WNL'