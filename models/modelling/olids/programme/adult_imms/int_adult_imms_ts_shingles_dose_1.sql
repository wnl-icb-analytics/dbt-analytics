{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}
--SHINGLES DOSE 1
-- SHINGLES TURN 65 FROM 1st SEPTEMBER 2023
with eligible_shing as (
select 
pmab.person_id, 
pmab.age_band_5y,
-- Ethnicity
pmab.ethnicity_category,
    CASE
        WHEN pmab.ethnicity_category = 'Asian' THEN 1
        WHEN pmab.ethnicity_category = 'Black' THEN 2
        WHEN pmab.ethnicity_category = 'Mixed' THEN 3
        WHEN pmab.ethnicity_category = 'Other' THEN 4
        WHEN pmab.ethnicity_category = 'White' THEN 5
        WHEN pmab.ethnicity_category = 'Unknown' THEN 6
    END AS ethcat_order,
    -- IMD
        COALESCE(pmab.imd_quintile_25, 'Unknown') AS imd_quintile,
        CASE
        WHEN pmab.imd_quintile_25 = 'Most Deprived' THEN 1
        WHEN pmab.imd_quintile_25 = 'Second Most Deprived' THEN 2
        WHEN pmab.imd_quintile_25 = 'Third Most Deprived' THEN 3
        WHEN pmab.imd_quintile_25 = 'Second Least Deprived' THEN 4
        WHEN pmab.imd_quintile_25 = 'Least Deprived' THEN 5
        ELSE 6
    END AS imdquintile_order,
    -- Practice
    pmab.practice_code,
    pmab.analysis_month, 
    pmab.financial_year as fiscal_year_label,
    CASE
    WHEN BIRTH_DATE_APPROX > '1957-09-01' AND BIRTH_DATE_APPROX <= '1958-09-01'
    THEN 'TURN_65_AFTER_SEP_2023' 
    WHEN AGE BETWEEN 70 AND 79 THEN '70-79 CATCH UP' END AS cohort,
    'Shingles Dose 1' AS VACCINE
FROM {{ ref('person_month_analysis_base') }} pmab
--FROM REPORTING.OLIDS_PERSON_ANALYTICS.PERSON_MONTH_ANALYSIS_BASE pmab
-- Limit to last 36 months (3 years)
WHERE pmab.analysis_month >= DATEADD('month', -36, CURRENT_DATE)
AND (BIRTH_DATE_APPROX > '1957-09-01' AND BIRTH_DATE_APPROX <= '1958-09-01' or AGE BETWEEN 70 AND 79)
)
,shingdose1 as (
select e.*, s1.shing_dose1_date, 
CASE
    WHEN SHING_DOSE1_DATE IS NOT NULL
         AND ANALYSIS_MONTH >= LAST_DAY(SHING_DOSE1_DATE)
    THEN 1
    ELSE 0
END AS VACCINATED
from eligible_shing e
LEFT JOIN {{ ref('int_adult_imms_historical_shingles_dose1') }} s1
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.int_adult_imms_historical_shingles_dose1 s1 using (PERSON_ID)
)
--aggregated output first dose 2 cohorts
select 
p.vaccine
,p.cohort
,p.analysis_month
,p.fiscal_year_label
,CASE
WHEN p.cohort ='TURN_65_AFTER_SEP_2023' THEN 3
WHEN p.cohort = '70-79 CATCH UP' THEN 4
END As VACC_ORDER 
,p.practice_code
,p.age_band_5y
,p.ethnicity_category
,p.ethcat_order
,p.imd_quintile
,p.imdquintile_order
,p.numerator 
,p.denominator 
FROM (
--TURN_65_AFTER_SEP_2023 
select 
vaccine, cohort, analysis_month, fiscal_year_label, practice_code, age_band_5y, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, 
sum(vaccinated) as numerator, count(*) as denominator 
FROM shingdose1
where cohort = 'TURN_65_AFTER_SEP_2023'
group by all

UNION
--70-79 CATCH UP
select 
vaccine, cohort, analysis_month, fiscal_year_label, practice_code, age_band_5y, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, 
sum(vaccinated) as numerator, count(*) as denominator 
FROM shingdose1
where cohort = '70-79 CATCH UP'
group by all
) p
