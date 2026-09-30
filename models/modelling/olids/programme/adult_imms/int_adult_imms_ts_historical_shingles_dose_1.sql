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
    'Shingles Dose 1' AS VACCINE
FROM {{ ref('person_month_analysis_base') }} pmab
--FROM REPORTING.OLIDS_PERSON_ANALYTICS.PERSON_MONTH_ANALYSIS_BASE pmab
-- Limit to last 24 months (2 years)
WHERE pmab.analysis_month >= DATEADD('month', -24, CURRENT_DATE)
AND (BIRTH_DATE_APPROX > '1957-09-01' AND BIRTH_DATE_APPROX <= '1958-09-01' or AGE BETWEEN 70 AND 79)
)
---SHINGLES IMMUNOSUPPRESSED Flags from September 2024 (134K rows)
, risk_flags AS (
SELECT PERSON_ID , valid_to, valid_from 
FROM (
    SELECT
        person_id,
        campaign_id,
        TO_DATE(REGEXP_SUBSTR(campaign_id, '[0-9]{4}') || '-09-01') AS valid_from,
        DATEADD(DAY,-1,LEAD(TO_DATE(REGEXP_SUBSTR(campaign_id, '[0-9]{4}') || '-09-01')) 
        OVER (PARTITION BY person_id ORDER BY campaign_id)) AS valid_to,
        CASE
            WHEN COALESCE(IS_IMMUNOSUPPRESSED, FALSE)
            THEN TRUE
            ELSE FALSE
        END AS IS_IMMUNOSUPPRESSED
    FROM {{ ref('int_covid_flu_risk_group_flags') }}
    --FROM MODELLING.OLIDS_PROGRAMME.INT_COVID_FLU_RISK_GROUP_FLAGS
    WHERE campaign_id LIKE 'Flu%'
) a WHERE  IS_IMMUNOSUPPRESSED --AND valid_from iS NOT NULL
)
--SHINGLES IMMUNOSUPPRESSED - no upper age limit
,eligible_shing_immunosuppressed as (
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
'Immunocompromised' AS cohort,
 'Shingles Dose 1' AS VACCINE
FROM {{ ref('person_month_analysis_base') }} pmab
--FROM REPORTING.OLIDS_PERSON_ANALYTICS.PERSON_MONTH_ANALYSIS_BASE pmab
INNER JOIN risk_flags rf
    ON pmab.person_id = rf.person_id
   AND pmab.analysis_month >= rf.valid_from
   AND ( pmab.analysis_month <= rf.valid_to
        OR rf.valid_to IS NULL    )
-- Limit to last 24 months (2 years for flu clinical risk flags)
WHERE pmab.analysis_month >= DATEADD('month', -24, CURRENT_DATE) 
)
--COMBINED ELIGIBLE COHORT AS 3.3 million rows
,all_shing_eligible as (
select *
from eligible_shing
UNION 
select *
FROM eligible_shing_immunosuppressed
)
--join with historical shingles dose 1 data
select e.*, s1.shing_dose1_date, 
CASE
    WHEN SHING_DOSE1_DATE IS NOT NULL
         AND ANALYSIS_MONTH >= LAST_DAY(SHING_DOSE1_DATE)
    THEN 1
    ELSE 0
END AS VACCINATED
from all_shing_eligible e
LEFT JOIN {{ ref('int_adult_imms_historical_shingles_dose1') }} s1 using (PERSON_ID)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.int_adult_imms_historical_shingles_dose1 s1 using (PERSON_ID)
