{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}

--SMI REGISTER BASE POPULATION

WITH smi_diagnoses AS (
    SELECT
        person_id,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS latest_diagnosis_date,
        MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)
            AS latest_resolved_date
    FROM {{ ref('int_smi_diagnoses_all') }}
    WHERE clinical_effective_date <= CURRENT_DATE()
        AND (
            date_recorded IS NULL
            OR CAST(date_recorded AS DATE) <= CURRENT_DATE()
        )
    GROUP BY person_id
),

diagnosis_membership AS (
    SELECT
        person_id,
        COALESCE(latest_resolved_date >= latest_diagnosis_date, FALSE)
            AS has_recent_resolved_code,
        NOT COALESCE(latest_resolved_date >= latest_diagnosis_date, FALSE)
            AS has_active_smi_diagnosis
    FROM smi_diagnoses
    WHERE latest_diagnosis_date IS NOT NULL
),

lithium_stops AS (
    SELECT
        person_id,
        MAX(CAST(clinical_effective_date AS DATE)) AS latest_stop_date
    FROM ({{ get_observations("'LITSTP_COD'", source='PCD') }})
    WHERE clinical_effective_date <= CURRENT_DATE()
        AND (
            date_recorded IS NULL
            OR CAST(date_recorded AS DATE) <= CURRENT_DATE()
        )
    GROUP BY person_id
),

recent_lithium AS (
    SELECT
        person_id,
        MAX(CAST(order_date AS DATE)) AS latest_order_date
    FROM {{ ref('int_lithium_medications_all') }}
    WHERE CAST(order_date AS DATE) <= CURRENT_DATE()
        AND CAST(order_date AS DATE) > DATEADD('month', -6, CURRENT_DATE())
        AND (
            date_recorded IS NULL
            OR CAST(date_recorded AS DATE) <= CURRENT_DATE()
        )
    GROUP BY person_id
),

active_lithium AS (
    SELECT lithium.person_id
    FROM recent_lithium AS lithium
    LEFT JOIN lithium_stops AS stops
        ON lithium.person_id = stops.person_id
        AND stops.latest_stop_date > lithium.latest_order_date
    WHERE stops.person_id IS NULL
),

programme_membership AS (
    SELECT
        diagnosis.person_id,
        lithium.person_id IS NOT NULL AS is_on_lithium,
        diagnosis.has_active_smi_diagnosis,
        diagnosis.has_recent_resolved_code
    FROM diagnosis_membership AS diagnosis
    LEFT JOIN active_lithium AS lithium
        ON diagnosis.person_id = lithium.person_id

    UNION ALL

    SELECT
        lithium.person_id,
        TRUE AS is_on_lithium,
        FALSE AS has_active_smi_diagnosis,
        FALSE AS has_recent_resolved_code
    FROM active_lithium AS lithium
    LEFT JOIN diagnosis_membership AS diagnosis
        ON lithium.person_id = diagnosis.person_id
    WHERE diagnosis.person_id IS NULL
)

select
dem.PERSON_ID
,ID.HX_FLAKE
,dem.SK_PATIENT_ID
,dem.AGE
,dem.AGE_BAND_5Y
,dem.AGE_BAND_NHS
,dem.BIRTH_DATE_APPROX
,CASE
WHEN dem.age_band_nhs = '5-14' THEN 1
WHEN dem.age_band_nhs = '15-24' THEN 2
WHEN dem.age_band_nhs = '25-34' THEN 3
WHEN dem.age_band_nhs = '35-44' THEN 4
WHEN dem.age_band_nhs = '45-54' THEN 5
WHEN dem.age_band_nhs = '55-64' THEN 6
WHEN dem.age_band_nhs = '65-74' THEN 7
WHEN dem.age_band_nhs = '75-84' THEN 8
WHEN dem.age_band_nhs = '85+' THEN 9
END AS AGE_NHS_ORDER
,dem.GENDER
,CASE
WHEN dem.ETHNICITY_CATEGORY = 'Not Recorded' THEN 'Unknown'
ELSE dem.ETHNICITY_CATEGORY END AS ETHNICITY_CATEGORY
,CASE 
WHEN dem.ETHNICITY_CATEGORY = 'Asian' THEN 1
WHEN dem.ETHNICITY_CATEGORY = 'Black' THEN 2
WHEN dem.ETHNICITY_CATEGORY = 'Mixed' THEN 3
WHEN dem.ETHNICITY_CATEGORY = 'Other' THEN 4
WHEN dem.ETHNICITY_CATEGORY = 'White' THEN 5
WHEN dem.ETHNICITY_CATEGORY = 'Unknown' THEN 6
WHEN dem.ETHNICITY_CATEGORY = 'Not Recorded' THEN 6
END AS ETHCAT_ORDER 
,CASE
WHEN dem.ETHNICITY_SUBCATEGORY in ('Not Recorded','Not stated','Not Stated','Recorded Not Known','Refused') THEN 'Unknown'
ELSE dem.ETHNICITY_SUBCATEGORY END AS ETHNICITY_SUBCATEGORY
,CASE 
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Bangladeshi' THEN 1
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Chinese' THEN 2
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Indian' THEN 3
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Pakistani' THEN 4
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Other Asian' THEN 5
WHEN dem.ETHNICITY_SUBCATEGORY = 'Black: African' THEN 6
WHEN dem.ETHNICITY_SUBCATEGORY = 'Black: Caribbean' THEN 7
WHEN dem.ETHNICITY_SUBCATEGORY = 'Black: Other Black' THEN 8
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: White and Asian' THEN 9
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: White and Black African' THEN 10
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: White and Black Caribbean' THEN 11
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: Other Mixed' THEN 12
WHEN dem.ETHNICITY_SUBCATEGORY = 'Other: Arab' THEN 13
WHEN dem.ETHNICITY_SUBCATEGORY = 'Other: Other' THEN 14
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: British' THEN 15
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: Irish' THEN 16
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: Traveller' THEN 17
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: Other White' THEN 18
WHEN dem.ETHNICITY_SUBCATEGORY = 'Unknown' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Not Recorded' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Not stated' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Not Stated' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Recorded Not Known' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Refused' THEN 19
END AS ETHSUBCAT_ORDER
,CASE 
WHEN dem.ETHNICITY_GRANULAR = 'MENA' THEN 'Middle East and North African'
WHEN dem.ETHNICITY_GRANULAR in ('Muslim', 'Sikh') THEN 'Unknown'
WHEN dem.ETHNICITY_GRANULAR in ('Recorded Not Known', 'Refused', 'Not stated', 'Not Recorded','Not Stated') THEN 'Unknown'
WHEN dem.ETHNICITY_GRANULAR = 'Black - E.African Asian' THEN 'East African Asian'
WHEN dem.ETHNICITY_GRANULAR = 'Indo-Caribbean' THEN 'Caribbean Asian'
WHEN dem.ETHNICITY_GRANULAR = 'Cornish' THEN 'English'
WHEN dem.ETHNICITY_GRANULAR in ('Gypsy or Irish Traveller','Gypsy','Irish Traveller') THEN 'Gypsy or Irish Traveller'
WHEN dem.ETHNICITY_GRANULAR in ('Albanian/Serbian', 'Serbian') THEN 'Albanian or Serbian'
ELSE dem.ETHNICITY_GRANULAR END AS ETHNICITY_GRANULAR
--switch to IMD25
,COALESCE(dem.IMD_QUINTILE_25, 'Unknown') AS IMD_QUINTILE
,CASE 
WHEN dem.IMD_QUINTILE_25 = 'Most Deprived' THEN 1
WHEN dem.IMD_QUINTILE_25 = 'Second Most Deprived' THEN 2
WHEN dem.IMD_QUINTILE_25 = 'Third Most Deprived' THEN 3
WHEN dem.IMD_QUINTILE_25 = 'Second Least Deprived' THEN 4
WHEN dem.IMD_QUINTILE_25 = 'Least Deprived' THEN 5
ELSE 6 END AS IMDQUINTILE_ORDER
,dem.IMD_DECILE_25 AS IMD_DECILE
,CASE 
WHEN dem.MAIN_LANGUAGE = 'Pushto' THEN 'Pashto' 
WHEN dem.MAIN_LANGUAGE = 'Gujerati' THEN 'Gujarati'
WHEN dem.MAIN_LANGUAGE ILIKE '%sign language%' THEN 'Sign language'
WHEN dem.MAIN_LANGUAGE = 'Norwegian Bokmål' THEN 'Norwegian'
WHEN dem.MAIN_LANGUAGE = 'Not Recorded' THEN 'Unknown'
ELSE dem.MAIN_LANGUAGE END AS MAIN_LANGUAGE
,dem.INTERPRETER_NEEDED
,dem.INTERPRETER_TYPE
,CASE WHEN HOM.PERSON_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_HOMELESS
,dem.BOROUGH_REGISTERED AS PRACTICE_BOROUGH 
,dem.NEIGHBOURHOOD_REGISTERED AS PRACTICE_NEIGHBOURHOOD
,dem.PCN_NAME AS PRIMARY_CARE_NETWORK
,dem.PRACTICE_NAME 
,dem.PRACTICE_CODE
,COALESCE(la.LAD25_NM,'Unknown') as RESIDENTIAL_BOROUGH
,COALESCE(dem.NEIGHBOURHOOD_RESIDENT,'Unknown') as RESIDENTIAL_NEIGHBOURHOOD
,dem.WARD_CODE
,dem.WARD_NAME
,dem.LSOA_CODE_21
,CASE WHEN la.RESIDENT_FLAG IS NULL THEN 'Unknown'
ELSE la.RESIDENT_FLAG END as RESIDENTIAL_LOC
,ltc.HAS_CORONARY_HEART_DISEASE as HAS_CHD
,ltc.HAS_CHRONIC_KIDNEY_DISEASE as HAS_CKD
,ltc.HAS_DIABETES 
,ltc.HAS_COPD
,ltc.HAS_HYPERTENSION as HAS_HYP
,ltc.HAS_STROKE_TIA as HAS_STIA
,ltc.HAS_HEART_FAILURE as HAS_HF
,ltc.HAS_PERIPHERAL_ARTERIAL_DISEASE as HAS_PAD
,smi.IS_ON_LITHIUM
,smi.HAS_ACTIVE_SMI_DIAGNOSIS
,smi.HAS_RECENT_RESOLVED_CODE
FROM {{ ref('dim_person_demographics') }} dem 
INNER JOIN programme_membership smi using (PERSON_ID)
LEFT JOIN {{ ref('dim_person_conditions') }} ltc using (PERSON_ID)
LEFT JOIN {{ ref('person_pseudo') }} AS ID  using (PERSON_ID)
LEFT JOIN {{ ref('stg_reference_lsoa21_ward25_lad25') }} la on la.LSOA21_CD = dem.LSOA_CODE_21
LEFT JOIN {{ ref('dim_person_homeless') }} hom using (PERSON_ID)
where dem.is_active = TRUE 
and dem.is_deceased = FALSE

