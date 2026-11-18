{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}
--Using the person_demographics for anyone under on or over 60 who has been ever been registered with an NCL practice (active or inactive) n~200,000
--Capture the PPV dose 1 vaccination events for this population
  SELECT 
        v.PERSON_ID
        ,v.AGE_65_PLUS
        ,v.AGE_75_PLUS
       ,v.AGE_BAND_5Y
        ,v.IS_CARE_HOME_RESIDENT
        ,v.IS_IMMUNOSUPPRESSED
        ,v.IN_PPV_CLINICAL_RISK_GROUP
         ,v.IN_RSV_CLINICAL_RISK_GROUP
        ,v.IS_PREGNANT
        ,v.ETHNICITY_CATEGORY
        ,v.ETHCAT_ORDER
        ,v.IMD_QUINTILE
        ,v.IMDQUINTILE_ORDER
        ,v.practice_code
        ,v.EVENT_DATE AS ppv_dose1_date 
        ,TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS ppv_dose1_sort
        ,v.FISCAL_YEAR as ppv_dose1_fiscal
        ,MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS ppv_dose1_label
        FROM {{ ref('int_adult_imms_dose_base') }} v
         --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_DOSE_BASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = 'PPV_1' AND v.EVENT_TYPE LIKE 'Admin%'
       --select latest PPV event for each person.
       QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY EVENT_DATE DESC) = 1