{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

with vacc_status_adult as (
SELECT
  cv.person_id
--PPV -----------------------SELECT CORRECT STATUS
,COALESCE(
    -- Rule 1 for those AGED 65 or OLDER prefer PPV_1
    MAX(CASE WHEN IN_PPV_CLINICAL_RISK_GROUP = FALSE AND vaccine_id = 'PPV_1' THEN vaccination_status END),
    MAX(CASE WHEN IN_PPV_CLINICAL_RISK_GROUP = FALSE AND vaccine_id = 'PPV_1B' THEN vaccination_status END),
    -- Rule 2 for those in a PPV clinical Risk group  prefer PPV_1B
    MAX(CASE WHEN IN_PPV_CLINICAL_RISK_GROUP AND vaccine_id = 'PPV_1B' THEN vaccination_status END),
    MAX(CASE WHEN IN_PPV_CLINICAL_RISK_GROUP AND vaccine_id = 'PPV_1' THEN vaccination_status END)   
) AS ppv_status_dose_1
,MAX(CASE WHEN vaccine_id in ('PPV_1','PPV_1B') THEN cv.vaccination_date END) as ppv_date_dose_1
,MAX(CASE WHEN vaccine_id in ('PPV_1','PPV_1B') THEN cv.AGE_AT_EVENT END) as ppv_age_event_dose_1
-- ,MAX(CASE WHEN vaccine_id in ('SHING_1','SHING_1B') THEN cv.EVENT_TYPE END) as shing_status_dose_1
-- Shingles Dose 1
  ,COALESCE(
-- Rule 1 for those turned 65 on after 1st sep 2023 and are not immunosuppressed prefer SHING_1
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND IS_IMMUNOSUPPRESSED = FALSE AND vaccine_id = 'SHING_1' THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND IS_IMMUNOSUPPRESSED = FALSE AND vaccine_id in ('SHING_1B','SHING_1C') THEN vaccination_status END),
 -- Rule 2 for those turned 65 BEFORE 1st sep 2023 and are not immunosuppressed prefer SHING_1B CATCH UP CAMPAIGN
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND IS_IMMUNOSUPPRESSED = FALSE AND vaccine_id = 'SHING_1B' THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND IS_IMMUNOSUPPRESSED = FALSE AND vaccine_id in ('SHING_1','SHING_1C') THEN vaccination_status END),  
-- Rule 3 for those turned 65 BEFORE 1st sep 2023 AND are immunosuppressed prefer SHING_1 OR SHING_1C
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND IS_IMMUNOSUPPRESSED AND vaccine_id in ('SHING_1','SHING_1C') THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND IS_IMMUNOSUPPRESSED AND vaccine_id = 'SHING_1B' THEN vaccination_status END),  
-- Rule 4 for those NOT turned 65 BEFORE 1st sep 2023 AND are immunosuppressed prefer SHING_1C
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND IS_IMMUNOSUPPRESSED AND vaccine_id = 'SHING_1C' THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND IS_IMMUNOSUPPRESSED AND vaccine_id in ('SHING_1','SHING_1B') THEN vaccination_status END)  
) AS shing_status_dose_1
,MAX(CASE WHEN vaccine_id in ('SHING_1','SHING_1B','SHING_1C') THEN cv.vaccination_date END) as shing_date_dose_1
,MAX(CASE WHEN vaccine_id in ('SHING_1','SHING_1B','SHING_1C') THEN cv.AGE_AT_EVENT END) as shing_age_event_dose_1
-- Shingles Dose 2
  ,COALESCE(
-- Rule 1 for those turned 65 on after 1st sep 2023 and are not immunosuppressed prefer SHING_2
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND IS_IMMUNOSUPPRESSED = FALSE AND vaccine_id = 'SHING_2' THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND IS_IMMUNOSUPPRESSED = FALSE AND vaccine_id in ('SHING_2B','SHING_2C') THEN vaccination_status END),
 -- Rule 2 for those turned 65 BEFORE 1st sep 2023 and are not immunosuppressed prefer SHING_2B CATCH UP CAMPAIGN
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND IS_IMMUNOSUPPRESSED = FALSE AND vaccine_id = 'SHING_2B' THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND IS_IMMUNOSUPPRESSED = FALSE AND vaccine_id in ('SHING_2','SHING_2C') THEN vaccination_status END),  
-- Rule 3 for those turned 65 BEFORE 1st sep 2023 AND are immunosuppressed prefer SHING_2 OR SHING_2C
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND IS_IMMUNOSUPPRESSED AND vaccine_id in ('SHING_2','SHING_2C') THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 AND IS_IMMUNOSUPPRESSED AND vaccine_id = 'SHING_2B' THEN vaccination_status END),  
-- Rule 4 for those NOT turned 65 BEFORE 1st sep 2023 AND are immunosuppressed prefer SHING_2C
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND IS_IMMUNOSUPPRESSED AND vaccine_id = 'SHING_2C' THEN vaccination_status END),
    MAX(CASE WHEN TURN_65_AFTER_SEP_2023 = FALSE AND IS_IMMUNOSUPPRESSED AND vaccine_id in ('SHING_2','SHING_2B') THEN vaccination_status END)          
) AS shing_status_dose_2
,MAX(CASE WHEN vaccine_id in ('SHING_2','SHING_2B','SHING_2C') THEN cv.vaccination_date END) as shing_date_dose_2
,MAX(CASE WHEN vaccine_id in ('SHING_2','SHING_2B','SHING_2C') THEN cv.AGE_AT_EVENT END) as shing_age_event_dose_2
--- RSV -----------------------SELECT CORRECT STATUS
  ,COALESCE(
    --RULE 1 WHEN IS_PREGNANT TRUMPS all 
    MAX(CASE WHEN IS_PREGNANT AND vaccine_id = 'RSV_1C' THEN vaccination_status END),
    MAX(CASE WHEN IS_PREGNANT AND vaccine_id in ('RSV_1','RSV_1B','RSV_1D') THEN vaccination_status END),
    -- Rule 2 ROUTINE for those AGED 75 or OLDER starting 1st April 2026 (not pregnant or in a care home or in a clinical risk group) prefer RSV_1
    MAX(CASE WHEN IS_CARE_HOME_RESIDENT = FALSE AND IS_PREGNANT = FALSE AND IN_RSV_CLINICAL_RISK_GROUP = FALSE AND vaccine_id = 'RSV_1' THEN vaccination_status END),
    MAX(CASE WHEN IS_CARE_HOME_RESIDENT = FALSE AND IS_PREGNANT = FALSE AND IN_RSV_CLINICAL_RISK_GROUP = FALSE AND vaccine_id in ('RSV_1B','RSV_1C','RSV_1D') THEN vaccination_status END),
 -- Rule 3 for those IN A CARE HOME FOR OLDER PEOPLE but NOT PREGNANT AND NOT in a clinical risk group prefer RSV_1B
    MAX(CASE WHEN IS_CARE_HOME_RESIDENT AND IS_PREGNANT = FALSE AND IN_RSV_CLINICAL_RISK_GROUP = FALSE AND vaccine_id = 'RSV_1B' THEN vaccination_status END),
    MAX(CASE WHEN IS_CARE_HOME_RESIDENT AND IS_PREGNANT = FALSE AND IN_RSV_CLINICAL_RISK_GROUP = FALSE AND vaccine_id in ('RSV_1','RSV_1C','RSV_1D') THEN vaccination_status END),    
-- Rule 4 for those NOT IN A CARE HOME FOR OLDER PEOPLE but ARE PREGNANT and not in a clinical risk group prefer RSV_1C
    MAX(CASE WHEN IS_CARE_HOME_RESIDENT = FALSE AND IS_PREGNANT AND IN_RSV_CLINICAL_RISK_GROUP = FALSE AND vaccine_id = 'RSV_1C' THEN vaccination_status END),
    MAX(CASE WHEN IS_CARE_HOME_RESIDENT = FALSE AND IS_PREGNANT AND IN_RSV_CLINICAL_RISK_GROUP = FALSE AND vaccine_id in ('RSV_1','RSV_1B','RSV_1D') THEN vaccination_status END),
-- Rule 5: those in an RSV clinical risk group and not in a care home and not pregant. Prefer RSV_1D.
    MAX(CASE WHEN IN_RSV_CLINICAL_RISK_GROUP = TRUE AND IS_PREGNANT = FALSE AND IS_CARE_HOME_RESIDENT = FALSE AND vaccine_id = 'RSV_1D' THEN vaccination_status END),
    MAX(CASE WHEN IN_RSV_CLINICAL_RISK_GROUP = TRUE AND IS_PREGNANT = FALSE AND IS_CARE_HOME_RESIDENT = FALSE AND vaccine_id IN ('RSV_1', 'RSV_1B', 'RSV_1C') THEN vaccination_status END),
-- Rule 6: those in an RSV clinical risk group under 65 and in a care home and not pregant. Prefer RSV_1B.
    MAX(CASE WHEN IN_RSV_CLINICAL_RISK_GROUP = TRUE AND IS_PREGNANT = FALSE AND IS_CARE_HOME_RESIDENT AND vaccine_id = 'RSV_1B' THEN vaccination_status END),
    MAX(CASE WHEN IN_RSV_CLINICAL_RISK_GROUP = TRUE AND IS_PREGNANT = FALSE AND IS_CARE_HOME_RESIDENT AND vaccine_id IN ('RSV_1', 'RSV_1D', 'RSV_1C') THEN vaccination_status END)
) AS rsv_status_dose_1
,MAX(CASE WHEN vaccine_id in ('RSV_1','RSV_1B','RSV_1C') THEN cv.vaccination_date END) as rsv_date_dose_1
,MAX(CASE WHEN vaccine_id in ('RSV_1','RSV_1B','RSV_1C') THEN cv.AGE_AT_EVENT END) as rsv_age_event_dose_1
FROM {{ ref('int_adult_imms_vaccination_status_current') }} cv
--FROM MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_VACCINATION_STATUS_CURRENT cv
GROUP BY cv.person_id

)
SELECT
CURRENT_DATE AS RUN_DATE
,v.person_id
,p.GENDER
,p.BIRTH_DATE_APPROX
,p.AGE
,p.AGE_BAND_5Y
,p.IS_CARE_HOME_RESIDENT
,p.IS_PREGNANT
,p.IS_IMMUNOSUPPRESSED
,p.TURN_65_AFTER_SEP_2023
,p.IN_PPV_CLINICAL_RISK_GROUP
,p.IN_RSV_CLINICAL_RISK_GROUP
,p.ethnicity_category
,p.ethcat_order
,p.ethnicity_subcategory
,p.ethsubcat_order
,p.ethnicity_granular 
,p.imd_quintile
,p.imdquintile_order
,p.imd_decile
,p.main_language
,p.practice_borough
,p.practice_neighbourhood
,p.primary_care_network
,p.gp_name
,p.practice_code
,p.RESIDENTIAL_BOROUGH
,p.residential_neighbourhood
,p.RESIDENTIAL_LOC
,p.ward_code
,p.ward_name
,p.lsoa_code_21
--extra vulnerabilities
,IFF(smi.person_id IS NOT NULL, 'Yes', 'No') AS has_smi
,IFF(hs.person_id IS NOT NULL, 'Yes', 'No') AS is_housebound
,v.ppv_status_dose_1 
,v.ppv_date_dose_1
,v.ppv_age_event_dose_1
,v.shing_status_dose_1 
,v.shing_date_dose_1
,v.shing_age_event_dose_1
,v.shing_status_dose_2 
,v.shing_date_dose_2
,v.shing_age_event_dose_2
,v.rsv_status_dose_1 
,v.rsv_date_dose_1
,v.rsv_age_event_dose_1
FROM vacc_status_adult v
INNER JOIN {{ ref('int_adult_imms_current_population') }} p using (person_id)
--LEFT JOIN REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_HOUSEBOUND_STATUS hs ON p.person_id = hs.person_id
LEFT JOIN {{ ref('dim_person_housebound_status') }} hs ON p.person_id = hs.person_id
--LEFT JOIN REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_SMI_REGISTER smi on p.person_id = smi.person_id
LEFT JOIN {{ ref('fct_person_smi_register') }} smi on p.person_id = smi.person_id
