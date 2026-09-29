{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}
/* unaggregated model 9.6 million rows */
//ELIGIBLE PPV AGE COHORT(5.1 million rows)
with eligible_ppv_age as (
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
 CASE WHEN AGE >=65 THEN 'AGE 65+' END AS cohort,
 'PPV Dose 1' AS VACCINE
FROM {{ ref('person_month_analysis_base') }} pmab
--FROM REPORTING.OLIDS_PERSON_ANALYTICS.PERSON_MONTH_ANALYSIS_BASE pmab
-- Limit to last 24 months (2 years)
WHERE analysis_month >= DATEADD('month', -24, CURRENT_DATE)
AND AGE >=65
)
---PPV RISK Flags from September 2024 DOES NOT INCLUDE Cochlear Implant or CSF Leak (4.5 million rows)
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
              OR COALESCE(HAS_ASPLENIA, FALSE)
              OR COALESCE(HAS_CHD, FALSE)
              OR COALESCE(HAS_CKD, FALSE)
              OR COALESCE(HAS_CLD, FALSE)
              OR COALESCE(HAS_DIABETES, FALSE)
              OR COALESCE(IS_IMMUNOSUPPRESSED, FALSE)
            THEN TRUE
            ELSE FALSE
        END AS HAS_PPV_CLINICAL_RISK_GROUP
    FROM {{ ref('int_covid_flu_risk_group_flags') }}
    --FROM MODELLING.OLIDS_PROGRAMME.INT_COVID_FLU_RISK_GROUP_FLAGS
    WHERE campaign_id LIKE 'Flu%'
) a WHERE  HAS_PPV_CLINICAL_RISK_GROUP --AND valid_from iS NOT NULL
)
--PPV Risk group limited by age 18-64 (4.5 million rows)
,eligible_ppv_risk_group as (
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
'CLINICAL RISK 18-64' AS cohort,
 'PPV' AS VACCINE
FROM {{ ref('person_month_analysis_base') }} pmab
--FROM REPORTING.OLIDS_PERSON_ANALYTICS.PERSON_MONTH_ANALYSIS_BASE pmab
INNER JOIN risk_flags rf
    ON pmab.person_id = rf.person_id
   AND pmab.analysis_month >= rf.valid_from
   AND ( pmab.analysis_month <= rf.valid_to
        OR rf.valid_to IS NULL    )
-- Limit to last 24 months (2 years for flu clinical risk flags)
WHERE pmab.analysis_month >= DATEADD('month', -24, CURRENT_DATE) AND pmab.AGE BETWEEN 18 AND 64 
)
--COMBINED ELIGIBLE COHORT AS 9.6 million rows
,all_ppv_eligible as (
select *
from eligible_ppv_age
UNION 
select *
FROM eligible_ppv_risk_group
)
--combine with historical ppv doses
-- ,ppv_vaccinated as (
select e.*, p1.ppv_dose1_date, 
CASE
    WHEN PPV_DOSE1_DATE IS NOT NULL
         AND ANALYSIS_MONTH >= LAST_DAY(PPV_DOSE1_DATE)
    THEN 1
    ELSE 0
END AS vaccinated
from all_ppv_eligible e
LEFT JOIN {{ ref('int_adult_imms_historical_ppv_dose1') }} p1 using (PERSON_ID)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.int_adult_imms_historical_ppv_dose1 p1 using (PERSON_ID)