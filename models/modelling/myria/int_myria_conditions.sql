{{ 
    config(
        materialized='table',
        tags='daily')
    }}

with most_recent_bh_admission as -- gets most recent non-elective BH admission for local patient identifier
(
    SELECT
    a.patient_id,
    a.local_patient_identifier,
    a.activity_date, -- discharge date
    a.activity_start_date, -- admission date
    a.primary_id as most_recent_bh_primary_id,
    ROW_NUMBER() OVER(PARTITION BY a.patient_id ORDER BY a.activity_date desc, a.diag_n asc) as appt_order
    FROM {{ ref("int_myria_attendances_diagnoses") }} a
    WHERE
    a.provider_site_code = 'RAL26' -- Barnet Hospital
    AND a.POD IN ('NEL-ZLOS','NEL-LOS+1')
    ),
most_recent_nel_admission as
(
    SELECT
    a.patient_id,
    a.local_patient_identifier,
    a.activity_date, -- discharge date
    a.activity_start_date, -- admission date
    a.age_at_event,
    a.primary_id as most_recent_nel_primary_id,
    ROW_NUMBER() OVER(PARTITION BY a.patient_id ORDER BY a.activity_date desc, a.diag_n asc) as appt_order
    FROM {{ ref("int_myria_attendances_diagnoses") }} a
    WHERE
    a.POD IN ('NEL-ZLOS','NEL-LOS+1')
    ),
pds_patient_check as
(
    SELECT
    pp.*,
    gp.practice_code,
    gp.practice_name,
    gp.local_authority
    FROM {{ ref("stg_pds_person") }} pp
    LEFT JOIN {{ ref("stg_pds_patient_care_practice") }} pc
        ON pp.sk_patient_id = pc.sk_patient_id
        AND pc.event_to_date IS NULL
    LEFT JOIN {{ ref("dim_practice_neighbourhood") }} gp ON pc.practice_code = gp.practice_code -- find most recent practice information
    WHERE
    pp.event_to_date IS NULL -- current patient state
    )
 SELECT 
    att_dx.patient_id,
    bh.local_patient_identifier as hospital_number_most_recent_nel_bh,
    nel.local_patient_identifier as hospital_number_most_recent_nel_any_provider,
    TO_VARCHAR(ARRAY_AGG(NCL.LOCAL_AUTHORITY) WITHIN GROUP (ORDER BY att_dx.activity_date desc)[0]) AS local_authority, -- gets most recent registered local authority
    TO_VARCHAR(ARRAY_AGG(NCL.PRACTICE_CODE) WITHIN GROUP (ORDER BY att_dx.activity_date desc)[0]) AS gp_code,
    TO_VARCHAR(ARRAY_AGG(NCL.PRACTICE_NAME) WITHIN GROUP (ORDER BY att_dx.activity_date desc)[0]) AS gp_name,
    ppc.local_authority as local_authority_pds,
    ppc.practice_code as gp_code_pds,
    ppc.practice_name as gp_name_pds,
    nel.age_at_event AS age_at_most_recent_nel_admission,
    nel.activity_start_date AS most_recent_nel_admission_date,
    nel.activity_date AS most_recent_nel_discharge_date,
    bh.activity_start_date AS most_recent_nel_admission_date_bh,
    bh.activity_date AS most_recent_nel_discharge_date_bh,
    CASE -- counts distinct attendance IDs and then flags as 1 if there is at least 1 non-elective attendance at Barnet Hospital in the period
        WHEN COUNT(DISTINCT 
                    CASE 
                        WHEN provider_site_code = 'RAL26' -- Barnet Hospital
                            AND att_dx.activity_date >= '01-Jan-2025' AND att_dx.activity_date < DATE_TRUNC('month',CURRENT_DATE) -- activity between 1 Jan 2025 and start of current month
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) >= 1 
        THEN 1
        ELSE 0 END AS barnet_hospital_flag,

    COUNT(DISTINCT -- counts distinct attendance IDs for non-elective attendances at Barnet Hospital in the period
                    CASE 
                        WHEN provider_site_code = 'RAL26' -- Barnet Hospital
                            AND att_dx.activity_date >= '01-Jan-2025' AND att_dx.activity_date < DATE_TRUNC('month',CURRENT_DATE)
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) as barnet_hospital_count,

    CASE -- counts distinct attendance IDs and then flags as 1 if there is at least 1 non-elective attendance at non-Barnet Hospital site in the period
        WHEN COUNT(DISTINCT 
                    CASE 
                        WHEN provider_code IN ('RAL','RAP')
                            AND provider_site_code <> 'RAL26' -- exclude Barnet Hospital
                            AND att_dx.activity_date >= '01-Jan-2025' AND att_dx.activity_date < DATE_TRUNC('month',CURRENT_DATE)
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) >= 1 
        THEN 1 
        ELSE 0 END AS RFL_ex_BH_flag,

    COUNT(DISTINCT -- counts distinct attendance IDs for non-elective attendances at non-Barnet Hospital sites in the period
                    CASE 
                        WHEN provider_code IN ('RAL','RAP')
                            AND provider_site_code <> 'RAL26' -- exclude Barnet Hospital 
                            AND att_dx.activity_date >= '01-Jan-2025' AND att_dx.activity_date < DATE_TRUNC('month',CURRENT_DATE)
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) AS RFL_ex_BH_count,

    CASE -- counts distinct attendance IDs and then flags as 1 if there is at least 1 non-elective attendance at RFL in the period
        WHEN COUNT(DISTINCT 
                    CASE 
                        WHEN provider_code IN ('RAL','RAP')
                            AND att_dx.activity_date >= '01-Jan-2025' AND att_dx.activity_date < DATE_TRUNC('month',CURRENT_DATE)
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) >= 1 
        THEN 1 
        ELSE 0 END AS RFL_flag,
    
    COUNT(DISTINCT -- counts distinct attendance IDs at RFL in the period
                    CASE 
                        WHEN provider_code IN ('RAL','RAP')
                            AND att_dx.activity_date >= '01-Jan-2025' AND att_dx.activity_date < DATE_TRUNC('month',CURRENT_DATE)
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) AS RFL_count,
    
    CASE -- counts distinct attendance IDs and then flags as 1 if there is at least 1 non-elective attendance at an NCL provider in the period
        WHEN COUNT(DISTINCT 
                    CASE 
                        WHEN provider_code IN ('RAL','RAP','RKE','RRV')
                            AND att_dx.activity_date >= '01-Jan-2025' AND att_dx.activity_date < DATE_TRUNC('month',CURRENT_DATE)
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) >= 1 
        THEN 1 
        ELSE 0 END AS NCLProvider_flag,
    
    COUNT(DISTINCT -- counts distinct attendance IDs at NCL providers in the period
                    CASE 
                        WHEN provider_code IN ('RAL','RAP','RKE','RRV')
                            AND att_dx.activity_date >= '01-Jan-2025' AND att_dx.activity_date < DATE_TRUNC('month',CURRENT_DATE)
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) AS NCLProvider_count,

    CASE -- counts distinct attendance IDs and then flags as 1 if there is at least 1 non-elective attendance at a non-NCL provider in the period
        WHEN COUNT(DISTINCT
                    CASE
                        WHEN provider_code NOT IN ('RAL','RAP','RKE','RRV')
                            AND att_dx.activity_date >= '01-Jan-2025' AND att_dx.activity_date < DATE_TRUNC('month',CURRENT_DATE)
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) >= 1 
        THEN 1 
        ELSE 0 END AS Non_NCLProvider_flag,
    
    COUNT(DISTINCT -- counts distinct attendance IDs non-NCL providers in the period
                    CASE
                        WHEN provider_code NOT IN ('RAL','RAP','RKE','RRV')
                            AND att_dx.activity_date >= '01-Jan-2025' AND att_dx.activity_date < DATE_TRUNC('month',CURRENT_DATE)
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1')
                        THEN primary_id
                        END) AS Non_NCLProvider_count,
    -- activity counts for tiering
    COUNT(DISTINCT 
                    CASE
                        WHEN activity_months_ago between 0 and 24
                        --fin_year IN ('2023/24','2024/25')
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) AS nel_ip_admissions_last_24_months,
    
    COUNT(DISTINCT 
                    CASE
                        WHEN activity_months_ago between 0 and 12
                        -- fin_year IN ('2024/25')
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) AS nel_ip_admissions_last_12_months,
        
    COUNT(DISTINCT 
                    CASE
                        WHEN activity_months_ago between 0 and 6
                        --fin_year IN ('2024/25') AND fin_month BETWEEN 7 AND 12
                            AND pod IN ('NEL-ZLOS','NEL-LOS+1') 
                        THEN primary_id 
                        END) AS nel_ip_admissions_last_6_months,
    
    -- Severe Heart Failure → ICD-10: I50* (heart failure). “Severe” typically requires a proxy (e.g., ≥2 admissions in 12 months with I50*, cardiogenic shock, acute pulmonary oedema). Notes: ICD-10 itself doesn’t grade severity. (icdlist.com)
    -- Heart Failure → I50*. (icdlist.com)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('I50') THEN 1 ELSE 0 END) AS heart_failure,

    -- Intermediate Frailty Risk (HFRS) → Use the published Hospital Frailty Risk Score ICD-10 list (109 codes) and compute the HFRS; threshold 5–15 = intermediate. (The Lancet, bmjpublichealth.bmj.com, British Geriatrics Society)

    -- Severe COPD → ICD-10: J44* (COPD) ± J43* (emphysema). “Severe” proxy: frequent COPD admissions, COPD with acute respiratory failure (J96.0*/J96.1*), or ICU/ventilation codes. See NRAP COPD audit inclusions for practical J44 subcodes. (icdlist.com, RCP)
    -- COPD → J44* ± J43*. (icdlist.com)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('J44','J43') THEN 1 ELSE 0 END) AS copd,

    -- Neurological Organ Failure → Not a standard ICD-10 term. If you need a working definition, many programmes use “progressive neurological disease” AS a proxy, e.g., G12.2 (MND), G35* (MS), advanced Parkinson’s G20* subcodes (UK now sub-categorised), or coma/encephalopathy if that’s your intent. Recommend agreeing a local definition before coding. (icdlist.com, Sprypt)

    -- Dementia → F00* (Alzheimer’s), F01* (vascular), F02* (other diseases), F03* (unspecified) and G30* (Alzheimer disease). (UK 5th ed. browser shows details.) (classbrowser.nhs.uk)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('F00','F01','F02','F03','G30') THEN 1 ELSE 0 END) AS dementia,

    -- End Stage Renal Failure → N18.6 (ESRD) plus Z-code Z99.2 (dependence on dialysis). OPCS-4 dialysis also appears IN SUS APC (e.g., for haemodialysis/peritoneal dialysis—use a dialysis OPCS list). (DiseaseDB.com, ICDcodes.ai, opencodelists.org)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 5) IN ('N18.6','Z99.2') THEN 1 ELSE 0 END) AS end_stage_renal_failure,

    -- Severe Interstitial Lung Disease → J84* (other interstitial pulmonary diseases) with IPF J84.1*. “Severe” proxy: concurrent J96.1* (chronic respiratory failure), long-term oxygen therapy, or frequent ILD admissions. (icdlist.com, ICDcodes.ai)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('J84') THEN 1 ELSE 0 END) AS severe_interstitial_lung_disease,

    -- Parkinson’s Disease → G20* (now subdivided IN recent updates). (icdlist.com)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('G20') THEN 1 ELSE 0 END) AS parkinsons_disease,

    -- Chronic Kidney Disease (CKD) → N18.1–N18.5 (stages 1–5), N18.9 (unspecified), N18.6 (ESRD). (See UK guidance; many programmes use N18.* AS the base.) (icdlist.com, LKN)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('N18') THEN 1 ELSE 0 END) AS chronic_kidney_disease,

    -- Liver Failure → K72* (hepatic failure). Alcoholic hepatic failure specifically K70.4. (classbrowser.nhs.uk)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('K72') or LEFT(UPPER(diag_code), 5) IN ('K70.4') THEN 1 ELSE 0 END) AS liver_failure,

    -- Alcohol Dependence → F10.2* (alcohol dependence syndrome—use the 4th/5th-character subcodes where present). (classbrowser.nhs.uk, aapc.com)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 5) IN ('F10.2') THEN 1 ELSE 0 END) AS alcohol_dependence,

    -- Bronchiectasis → J47*. (classbrowser.nhs.uk)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('J47') THEN 1 ELSE 0 END) AS bronchiectasis,

    -- Atrial Fibrillation → I48* (AF and flutter). (NICE coding annexes also reference I48.* for AF episodes and ablation cohorts.) (icdlist.com, NICE)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('I48') THEN 1 ELSE 0 END) AS atrial_fibrillation,

    -- Physical Disability → Common functional status codes: Z74* (problems related to care-provider dependency, need for assistance), Z99.3 (dependence on wheelchair). Define locally which Z-codes “count.” (icdlist.com, ICDcodes.ai)
    -- NEED TO REFINE DEFINITION

    -- Cerebrovascular Disease (CVD) → I60*–I69* (haemorrhage, infarction, occlusions, sequelae). (classbrowser.nhs.uk)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('I60','I61','I62','I63','I64','I65','I66','I67','I68','I69') THEN 1 ELSE 0 END) AS cerebrovascular_disease,

    -- Peripheral Vascular Disease (PVD) → Often taken AS I70.2* (peripheral atherosclerosis), I73.* (other peripheral vascular diseases), I74.* (arterial embolism/thrombosis). Many studies use broader I70–I79 arterial disease block; be explicit. (classbrowser.nhs.uk)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 5) IN ('I70.2') OR LEFT (upper(diag_code), 3) IN ('I73','I74') THEN 1 ELSE 0 END) AS peripheral_vascular_disease,

    -- Pulmonary Heart Disease → I26*–I28* (PE, pulmonary hypertension, cor pulmonale). (classbrowser.nhs.uk, icdlist.com)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('I26','I27','I28') THEN 1 ELSE 0 END) AS pulmonary_heart_disease,

    -- Coronary Heart Disease (CHD) → I20*–I25* (angina through chronic ischaemic heart disease/MI sequelae). (classbrowser.nhs.uk)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('I20','I21','I22','I23','I24','I25') THEN 1 ELSE 0 END) AS coronary_heart_disease,

    -- Osteoporosis → M80* (with pathological fracture), M81* (without fracture). (classbrowser.nhs.uk, Bodyspec)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('M80','M81') THEN 1 ELSE 0 END) AS osteoporosis,

    -- Rheumatoid Arthritis → M05* (seropositive RA), M06* (other RA). (classbrowser.nhs.uk)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('M05','M06') THEN 1 ELSE 0 END) AS rheumatoid_arthritis,
    
    -- Chronic Liver Disease → Common public-health definition: K70, K73–K74 (often used for CLD monitoring). Some programmes include broader K70–K77 or aetiology-specific sets (viral B15–B19, etc.). Be clear which you adopt. (NHS England Digital, classbrowser.nhs.uk, GOV.UK)
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('K70','K73','K74') THEN 1 ELSE 0 END) AS chronic_liver_disease, 
    --- NON HIGH RISK CONDITIONS BUT HELPFUL FLAGS
    
    -- I10-I1A - Hypertensive diseases
    MAX(CASE WHEN LEFT(UPPER(diag_code), 2) IN ('I1') THEN 1 ELSE 0 END) AS hypertension, 
    
    -- R54 - Age-related physical debility, Z91.81 - History of falling
    MAX(CASE WHEN LEFT(UPPER(diag_code), 3) IN ('R54') OR UPPER(diag_code) IN ('Z91.81', 'Z9181') THEN 1 ELSE 0 END) AS frailty_falls,

    CASE WHEN ppc.practice_code IS NULL THEN 0 ELSE 1 END AS is_on_pds, -- flag NCL gps

    CASE WHEN ppc.date_of_death IS NOT NULL THEN 1 ELSE 0 END AS is_dead_pds,

    CASE WHEN death.sk_patient_id IS NOT NULL THEN 1 ELSE 0 END AS is_dead_death_registry,

    CURRENT_TIMESTAMP() AS refresh_date
FROM 
    {{ ref("int_myria_attendances_diagnoses") }} att_dx
INNER JOIN
    {{ ref("dim_practice_neighbourhood") }} ncl -- REPORTING.OLIDS_ORGANISATION.DIM_PRACTICE_NEIGHBOURHOOD AS ncl 
    ON att_dx.gp_code = ncl.PRACTICE_CODE
LEFT JOIN
    {{ ref("stg_registries_deaths") }} death
    ON att_dx.patient_id = death.sk_patient_id -- check whether patient dead as of running model
LEFT JOIN most_recent_bh_admission bh ON att_dx.patient_id = bh.patient_id
    AND bh.appt_order = 1
LEFT JOIN most_recent_nel_admission nel ON att_dx.patient_id = nel.patient_id
    AND nel.appt_order = 1
LEFT JOIN pds_patient_check ppc ON att_dx.patient_id = ppc.sk_patient_id
WHERE
    att_dx.patient_id IS NOT null
GROUP BY 
    ALL
