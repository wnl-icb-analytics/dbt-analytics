{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}
//ELIGIBLE RSV FIRST (2.3 million rows)
with eligible_rsv_age as (
--PPV - AGE COHORT
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
 CASE WHEN AGE >=65 THEN 'AGE 75+' END AS cohort,
 'RSV' AS VACCINE
FROM {{ ref('person_month_analysis_base') }} pmab
--FROM REPORTING.OLIDS_PERSON_ANALYTICS.PERSON_MONTH_ANALYSIS_BASE pmab
-- Limit to last 24 months (2 years)
WHERE analysis_month >= DATEADD('month', -24, CURRENT_DATE)
AND AGE >=75
)
---RSV RISK Flags from September 1st 2026 has chronic lung disease or is immunosuppressed
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
            WHEN COALESCE(HAS_CRD, FALSE)
              OR COALESCE(IS_IMMUNOSUPPRESSED, FALSE)
            THEN TRUE
            ELSE FALSE
        END AS HAS_RSV_CLINICAL_RISK_GROUP
    FROM {{ ref('int_covid_flu_risk_group_flags') }}
    --FROM MODELLING.OLIDS_PROGRAMME.INT_COVID_FLU_RISK_GROUP_FLAGS
    WHERE campaign_id LIKE 'Flu%'
) a WHERE  HAS_RSV_CLINICAL_RISK_GROUP --AND valid_from iS NOT NULL
)
--PPV Risk group for the last 2 years using the Covid and Flu flags table (4.5 million rows)
,eligible_rsv_risk_group as (
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
    'CLINICAL RISK 65-74' AS cohort,
     'RSV' AS VACCINE
FROM {{ ref('person_month_analysis_base') }} pmab
--FROM REPORTING.OLIDS_PERSON_ANALYTICS.PERSON_MONTH_ANALYSIS_BASE pmab
INNER JOIN risk_flags rf
    ON pmab.person_id = rf.person_id
   AND pmab.analysis_month >= rf.valid_from
   AND ( pmab.analysis_month <= rf.valid_to
        OR rf.valid_to IS NULL    )
-- Limit to last 24 months (2 years for flu clinical risk flags)
WHERE pmab.analysis_month >= DATEADD('month', -24, CURRENT_DATE) AND pmab.AGE BETWEEN 65 AND 74
)
--COMBINED ELIGIBLE COHORT AS 12 million rows
,all_rsv_eligible as (
select *
from eligible_rsv_age
UNION 
select *
FROM eligible_rsv_risk_group
)
--combine with historical ppv doses
,rsv_vaccinated as (
select e.*, r1.rsv_dose1_date, 
CASE
    WHEN RSV_DOSE1_DATE IS NOT NULL
         AND ANALYSIS_MONTH >= LAST_DAY(RSV_DOSE1_DATE)
    THEN 1
    ELSE 0
END AS vaccinated
from all_rsv_eligible e
LEFT JOIN {{ ref('int_adult_imms_historical_rsv_dose1') }} r1
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.int_adult_imms_historical_rsv_dose1 r1 using (PERSON_ID)
)
--aggregated output one dose 2 cohorts
select 
p.vaccine
,p.cohort
,p.analysis_month
,p.fiscal_year_label
,CASE
WHEN p.cohort ='AGE 75+' THEN 7
WHEN p.cohort ='CLINICAL RISK 65-74' THEN 8
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
--Age 75+ 
select 
vaccine, cohort, analysis_month, fiscal_year_label, practice_code, age_band_5y, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, 
sum(vaccinated) as numerator, count(*) as denominator 
FROM rsv_vaccinated
where cohort = 'AGE 75+'
group by all

UNION
--CLINICAL RISK 65-74
select 
vaccine, cohort, analysis_month, fiscal_year_label, practice_code, age_band_5y, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, 
sum(vaccinated) as numerator, count(*) as denominator 
FROM rsv_vaccinated
where cohort = 'CLINICAL RISK 65-74'
group by all
) p

