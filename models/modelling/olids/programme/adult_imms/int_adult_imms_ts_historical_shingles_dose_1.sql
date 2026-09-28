{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}
/* unaggregated model 2.5 million rows */
--SHINGLES DOSE 1
-- SHINGLES TURN 65 FROM 1st SEPTEMBER 2023 OR CATCH UP 70-79
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
-- Limit to last 24 months (2 years)
WHERE pmab.analysis_month >= DATEADD('month', -24, CURRENT_DATE)
AND (BIRTH_DATE_APPROX > '1957-09-01' AND BIRTH_DATE_APPROX <= '1958-09-01' or AGE BETWEEN 70 AND 79)
)
--join with historical shingles dose 1 data
select e.*, s1.shing_dose1_date, 
CASE
    WHEN SHING_DOSE1_DATE IS NOT NULL
         AND ANALYSIS_MONTH >= LAST_DAY(SHING_DOSE1_DATE)
    THEN 1
    ELSE 0
END AS VACCINATED
from eligible_shing e
LEFT JOIN {{ ref('int_adult_imms_historical_shingles_dose1') }} s1 using (PERSON_ID)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.int_adult_imms_historical_shingles_dose1 s1 using (PERSON_ID)
