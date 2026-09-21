{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

--April 2026 amend rsv eligibilty for new rules starting 1st April 2026, RSV for pregnant women and also new shingles vaccine eligibility aged 18+ immunosupressed.
--Add in PPV Clinical Risk Groups which are similar to COVID but also with Cochlear Implant and CSF leak.
SELECT 
p.PERSON_ID,
p.BIRTH_DATE_APPROX,
p.AGE,
p.AGE_DAYS_APPROX,
p.AGE_BAND_5Y,
p.GENDER,
p.IS_CARE_HOME_RESIDENT,
p.IS_IMMUNOSUPPRESSED,
p.IN_PPV_CLINICAL_RISK_GROUP,
p.IN_RSV_CLINICAL_RISK_GROUP,
p.IS_PREGNANT,
p.TURN_65_AFTER_SEP_2023,
-- p.TURN_75_AFTER_SEP_2024,
-- p.TURN_80_AFTER_SEP_2024,
sched.VACCINE_ORDER,
sched.VACCINE_ID,
sched.VACCINE_NAME,
sched.DOSE_NUMBER,
sched.ELIGIBLE_AGE_FROM_DAYS,
sched.ELIGIBLE_AGE_TO_DAYS,
sched.MAXIMUM_AGE_DAYS,
CASE 
--SHING_1 For people who turned 65 after 1st september 2023 - fixed start date
--SHING_2 For people who turned 65 after 1st september 2023 - fixed start date +6 months
--SHING_1C For immunosuppressed people aged 18+ - fixed start date
--SHING_2C For immunosuppressed people aged 18+ - fixed start date +6 months
WHEN sched.VACCINE_ID = 'SHING_1' THEN '2023-09-01'
WHEN sched.VACCINE_ID = 'SHING_2' THEN '2024-03-01'
WHEN sched.VACCINE_ID = 'SHING_1C' THEN '2025-09-01'
WHEN sched.VACCINE_ID = 'SHING_2C' THEN '2026-03-01'
ELSE DATE((BIRTH_DATE_APPROX + sched.ELIGIBLE_AGE_FROM_DAYS)) END AS ELIGIBLE_FROM_DATE,
DATE((BIRTH_DATE_APPROX + sched.ELIGIBLE_AGE_TO_DAYS)) AS ELIGIBLE_TO_DATE,
CASE 
--SHINGLES
WHEN p.TURN_65_AFTER_SEP_2023 AND sched.VACCINE_ID in ('SHING_1','SHING_2') AND CURRENT_DATE > '2023-09-01' 
AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN TRUE 
WHEN p.TURN_65_AFTER_SEP_2023 = FALSE AND sched.VACCINE_ID in ('SHING_1B','SHING_2B') AND AGE_DAYS_APPROX >= sched.eligible_age_from_days 
AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN TRUE 
WHEN p.IS_IMMUNOSUPPRESSED AND p.AGE >= 18 AND sched.VACCINE_ID in ('SHING_1C','SHING_2C') AND CURRENT_DATE > '2025-09-01' 
AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN TRUE 
--RSV
WHEN sched.VACCINE_ID = 'RSV_1' AND AGE_DAYS_APPROX >= sched.eligible_age_from_days AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN TRUE  
WHEN sched.VACCINE_ID = 'RSV_1B' AND IS_CARE_HOME_RESIDENT AND AGE_DAYS_APPROX >= sched.eligible_age_from_days 
AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN TRUE
WHEN sched.VACCINE_ID = 'RSV_1C' AND IS_PREGNANT AND AGE_DAYS_APPROX >= sched.eligible_age_from_days 
AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN TRUE
--add in RSV new clinical risk group from 1st September 2026
WHEN sched.VACCINE_ID = 'RSV_1D' AND IN_RSV_CLINICAL_RISK_GROUP AND AGE_DAYS_APPROX >= sched.eligible_age_from_days 
AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN TRUE
--PPV add in PPV clinical risk groups (includes immunosuppression) from age 2 +
WHEN sched.VACCINE_ID = 'PPV_1' AND AGE_DAYS_APPROX >= sched.eligible_age_from_days 
AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN TRUE 
WHEN sched.VACCINE_ID = 'PPV_1B' AND p.IN_PPV_CLINICAL_RISK_GROUP AND p.AGE >= 2 AND AGE_DAYS_APPROX >= sched.eligible_age_from_days 
AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN TRUE
ELSE FALSE END AS CURRENTLY_ELIGIBLE
FROM {{ ref('int_adult_imms_current_population') }} p
--FROM MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_CURRENT_POPULATION p
CROSS JOIN {{ ref('stg_reference_imms_schedule_adult_latest') }} sched
--CROSS JOIN MODELLING.DBT_STAGING.STG_REFERENCE_IMMS_SCHEDULE_ADULT_LATEST sched
WHERE 
AGE_DAYS_APPROX >= (select min(ELIGIBLE_AGE_FROM_DAYS) from {{ ref('stg_reference_imms_schedule_adult_latest') }}) 
--AGE_DAYS_APPROX >= (select min(ELIGIBLE_AGE_FROM_DAYS) from MODELLING.DBT_STAGING.STG_REFERENCE_IMMS_SCHEDULE_ADULT_LATEST) 