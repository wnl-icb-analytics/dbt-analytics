{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}
/* unaggregated model 2.5 million rows */
--SHINGLES DOSE 2
-- SHINGLES TURN 65 FROM 1st SEPTEMBER 2023 OR CATCH UP 70-79
with eligible_shing as (
select 
    pmab.person_id, 
    pmab.age_band_5y,
-- Ethnicity
    pmab.ethnicity_category,
    -- IMD
        COALESCE(pmab.imd_quintile_25, 'Unknown') AS imd_quintile,
    -- Practice
    pmab.practice_code,
    pmab.analysis_month, 
    pmab.financial_year as fiscal_year_label,
    CASE
    WHEN BIRTH_DATE_APPROX > '1957-09-01' AND BIRTH_DATE_APPROX <= '1958-09-01'
    THEN 'Aged 65 from Sep 2023' 
    WHEN AGE BETWEEN 70 AND 79 THEN 'Aged 70-79 Catch Up' END AS cohort,
    'Shingles Dose 2' AS VACCINE
FROM {{ ref('person_month_analysis_base') }} pmab
--FROM REPORTING.OLIDS_PERSON_ANALYTICS.PERSON_MONTH_ANALYSIS_BASE pmab
-- Limit to last 24 months (2 years)
WHERE pmab.analysis_month >= DATEADD(MONTH, -24, CURRENT_DATE)
AND (
       (
           pmab.birth_date_approx > DATE '1957-09-01'
       AND pmab.birth_date_approx <= DATE '1958-09-01'
       )
    OR pmab.age BETWEEN 70 AND 79
)
)
--join with historical shingles dose 2 data
select e.*, s2.shing_dose2_date, 
CASE
    WHEN SHING_DOSE2_DATE IS NOT NULL
         AND ANALYSIS_MONTH >= LAST_DAY(SHING_DOSE2_DATE)
    THEN 1
    ELSE 0
END AS VACCINATED
from eligible_shing e
LEFT JOIN {{ ref('int_adult_imms_historical_shingles_dose2') }} s2 using (PERSON_ID)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.int_adult_imms_historical_shingles_dose2 s2 using (PERSON_ID)