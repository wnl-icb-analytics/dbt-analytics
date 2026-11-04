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
--for non wnl activity can find which ICB the patient is registered with
,lookup_icb as (
select sk_organisation_id, 
REPLACE(
    REPLACE(organisation_name, 'NHS ', ''),' Integrated Care Board','') AS icb_name
FROM {{ ref('stg_dictionary_dbo_organisation')}}
--FROM STAGING.DICTIONARY.STG_DICTIONARY_DBO_ORGANISATION
where organisation_name like '%Integrated Care Board'
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
--build main table for last 3 financial years and rolling 12m flag
SELECT
--commissioning and primary care
u.sk_patient_id,
u.commissioner_code_at_event as commissioner_code,
IFF(u.commissioner_code_at_event IN ('07X','07M','08H','08D','07R','07P','07W','07Y','08C','08E','08G','08Y','09A','W2U3Z','93C','93C00','Z9B2Z','QRV','QRV00','QMJ','QMJ00'),'WNL','Other') comm_group,
u.reg_practice_at_event as practice_code,
CASE
    WHEN u.reg_practice_name_latest = 'HESA MEDICAL CENTRE - Y00352' THEN 'HESA Medical Centre'
    WHEN u.reg_practice_at_event in ('V81998','V81999') THEN 'Practice Code Unknown/NA'
    WHEN u.reg_practice_at_event = 'V81997' THEN 'Not registered with GP'
    ELSE
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                INITCAP(LOWER(u.reg_practice_name_latest)),
                '\\bGp\\b', 'GP'
            ),
            '\\bPcn\\b', 'PCN'
        )
END AS practice_name,
NVL(u.reg_practice_pcn_name_latest,'Not WNL/Unknown') as pcn_name,
NVL(u.reg_practice_neighbourhood_name_latest,'Not WNL/Unknown') as practice_neighbourhood,
NVL(u.reg_practice_health_borough_name_latest,'Not WNL/Unknown') as health_borough,
CASE WHEN u.reg_practice_health_borough_name_latest is null THEN FALSE ELSE TRUE END AS is_wnl_registered,
CASE 
WHEN u.reg_practice_at_event in ('E84023','E84624','E84667') THEN 'West and North London'
WHEN icb.icb_name IS NULL THEN 'Unknown'
ELSE icb.icb_name END AS icb_name,
--provider
u.organisation_id as provider_code,
wnl.provider_shorthand,
wnl.provider_name,
u.site_id,
CASE 
WHEN u.site_name = 'West Middlesex University Hospital Urgent Treatment Centre' THEN 'WMUH UTC'
WHEN u.site_name = 'Moorfields Eye Hospital (City Road Campus)' THEN 'Moorfields City Road'
WHEN u.site_name LIKE '%Desmond%' THEN 'Richard Desmond Eye Centre' ELSE u.site_name END AS site_name,
--department
--u.pod as dept_code,
CASE 
WHEN u.department_type = '01' THEN 'Major'
WHEN u.department_type = '02' THEN 'Mono'
WHEN u.department_type = '03' THEN 'UTC'
WHEN department_type = '04' THEN 'WIC'
WHEN department_type = '05' THEN 'SDEC'
ELSE department_type END as dept_type_desc,
--attendance dates
u.start_date as attendance_date,
u.financial_year,
CASE WHEN u.start_date >= DATEADD('month', -12, CURRENT_DATE) THEN TRUE ELSE FALSE END AS is_last_12m,
CASE 
        WHEN MONTH(u.start_date) >= 4 
        THEN YEAR(u.start_date) || '/' || RIGHT(YEAR(u.start_date) + 1, 2)
        ELSE (YEAR(u.start_date) - 1) || '/' || RIGHT(YEAR(u.start_date), 2)
    END as financial_year_label,
DATE_TRUNC('month', u.start_date) AS month_start_date,
TO_CHAR(u.start_date, 'mon-yy') AS month_label,
TO_NUMBER(TO_CHAR(u.start_date, 'YYYYMM')) AS month_sort,
u.week_end_date,
TO_CHAR(DATE_TRUNC('WEEK', u.week_end_date),'DD-MON-YY') AS week_label,
DAYNAME(u.start_date) AS day_of_week,
DAYOFWEEKISO(u.start_date) AS day_of_week_sort,
u.start_time,
DATE_PART('HOUR', u.start_time) AS hour_of_arrival,
u.duration,
--duration > 240 mins (4hrs)
u.is_four_hour_breach as is_4_hour_breach,
--duration > 720 mins (12 hrs)
u.duration >720 is_12_hour_breach,
--duration > 4320 mins (72 hrs)
u.duration > 4320 is_72_hour_breach,
u.arrival_mode_desc,
CASE
WHEN u.arrival_mode_desc LIKE '%ambulance%' THEN TRUE ELSE FALSE END AS arrival_by_ambulance,
--clinical details
u.hrg_code,
u.core_hrg_desc,
u.chief_complaint_desc,
u.chief_complaint_ecds_group1 as chief_complaint_group,
u.primary_diagnosis_desc_ecds_group1 as primary_diagnosis_group,
u.primary_treatment_desc_ecds_group1 as primary_treatment_group,
u.primary_investigation_desc_ecds_group1 as primary_investigation_group,
u.is_mental_health_related_attendance,
CASE
WHEN u.acuity_desc = 'Immediate resuscitation level emergency care' THEN 'Resuscitation'
WHEN u.acuity_desc = 'Very urgent level emergency care' THEN 'Very Urgent'
WHEN u.acuity_desc = 'Urgent level emergency care' THEN 'Urgent'
WHEN u.acuity_desc = 'Standard level emergency care' THEN 'Standard'
WHEN u.acuity_desc = 'Non-urgent level emergency care' THEN 'Non Urgent'
WHEN u.acuity_desc IS NULL THEN 'Unknown'
END AS acuity_level,
CASE
WHEN u.acuity_desc = 'Immediate resuscitation level emergency care' THEN 1
WHEN u.acuity_desc = 'Very urgent level emergency care' THEN 2
WHEN u.acuity_desc = 'Urgent level emergency care' THEN 3
WHEN u.acuity_desc = 'Standard level emergency care' THEN 4
WHEN u.acuity_desc = 'Non-urgent level emergency care' THEN 5
WHEN u.acuity_desc IS NULL THEN 6
END AS acuity_level_sort,
--outcome
discharge_destination_group as admission_status,
CASE
WHEN discharge_destination_group = 'Admitted' THEN TRUE ELSE FALSE END AS is_admitted,
--person_demographics
CASE WHEN g.lad25_nm IS NOT NULL AND g.residential_location = 'WNL' THEN TRUE ELSE FALSE END AS is_wnl_resident,
g.lad25_nm as borough_resident,
NVL(g.residential_location,'Unknown') as residence_group, 
-- 5-year age bands
-- CASE
--     WHEN u.age_at_event < 0 THEN 'Unknown'
--     WHEN u.age_at_event >= 85 THEN '85+'
--     ELSE TO_VARCHAR(FLOOR(u.age_at_event / 5) * 5) || '-' || TO_VARCHAR(FLOOR(u.age_at_event / 5) * 5 + 4)
-- END AS age_band_5y,
-- NHS Digital age bands (used in Health Survey for England)
CASE
    WHEN u.age_at_event < 0 OR u.age_at_event IS NULL THEN 'Unknown'
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
CASE
    WHEN u.age_at_event < 0 THEN 0
    WHEN u.age_at_event < 5 THEN 1
    WHEN u.age_at_event < 15 THEN 2
    WHEN u.age_at_event < 25 THEN 3
    WHEN u.age_at_event < 35 THEN 4
    WHEN u.age_at_event < 45 THEN 5
    WHEN u.age_at_event < 55 THEN 6
    WHEN u.age_at_event < 65 THEN 7
    WHEN u.age_at_event < 75 THEN 8
    WHEN u.age_at_event < 85 THEN 9
    ELSE 10
END AS age_sort,
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
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation')}} org on u.reg_practice_at_event = org.organisation_code
--LEFT JOIN STAGING.DICTIONARY.STG_DICTIONARY_DBO_ORGANISATION org on u.reg_practice_at_event = org.organisation_code
LEFT JOIN lookup_icb icb on icb.sk_organisation_id = org.sk_organisation_id_health_authority
WHERE u.start_date >= DATEADD(
    'year', -2,
    DATE_FROM_PARTS(
        YEAR(CURRENT_DATE) - IFF(MONTH(CURRENT_DATE) < 4, 1, 0),
        4,        1
    ) )
 