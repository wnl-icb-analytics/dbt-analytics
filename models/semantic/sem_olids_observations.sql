{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Clinical Observations Semantic View
    ==========================================

    Semantic model for clinical observations and biomarkers with
    clinically meaningful categories and control status. OLIDS is the
    One London Integrated Data Set — primary care data from system
    suppliers (currently EMIS Web, with TPP to follow), unified by the
    One London team.

    Grain: One row per person (latest observation values)

    Design Principles:
    - Categories over averages (% at target vs mean value)
    - Patient-specific thresholds where applicable (BP control)
    - Pre-computed clinical classifications from int/fct models

    Observation Groups:
    - Cardiovascular: BP, BP control, cholesterol, LDL, QRISK
    - Metabolic: HbA1c, BMI, waist circumference
    - Renal: eGFR (CKD staging), creatinine, urine ACR
    - Liver: ALT, GGT, bilirubin, composite high-LFT flag
    - Haematology: haemoglobin (anaemia), platelets, eosinophils
    - Frailty: calculated eFI2, GP-recorded eFI/eFI2, Rockwood Clinical Frailty Scale

    Diabetes care processes (foot exam, retinal screening, 8/9 care
    processes, triple target) live in sem_olids_diabetes_care.
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Base population - all persons with registration history',

    bp AS {{ ref('int_blood_pressure_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest blood pressure measurement',

    bp_control AS {{ ref('fct_person_bp_control') }}
        PRIMARY KEY (person_id)
        COMMENT = 'BP control status with patient-specific thresholds based on T2DM, CKD, age',

    hba1c AS {{ ref('int_hba1c_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest HbA1c with NICE-aligned clinical categories',

    cholesterol AS {{ ref('int_cholesterol_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest total cholesterol measurement',

    ldl AS {{ ref('int_cholesterol_ldl_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest LDL cholesterol measurement',

    bmi AS {{ ref('int_bmi_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest BMI with ethnicity-adjusted categories per NICE NG246',

    waist AS {{ ref('int_waist_circumference_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest waist circumference with risk categories',

    egfr AS {{ ref('int_egfr_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest eGFR with CKD staging',

    creatinine AS {{ ref('int_creatinine_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest serum creatinine measurement',

    qrisk AS {{ ref('int_qrisk_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest QRISK cardiovascular risk score',

    acr AS {{ ref('int_urine_acr_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest urine albumin:creatinine ratio',

    efi AS {{ ref('int_efi_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest GP-recorded Electronic Frailty Index (eFI or coded eFI2), as coded by the practice. Superseded by the calculated efi2_score below: this is recorded for only ~10k people against ~258k scored by the pipeline, and reflects whenever the practice last coded it rather than the current data. Use it only when the question is specifically about what practices have recorded. For clinical-diagnosis frailty prevalence use has_frailty from sem_olids_population.',

    efi2 AS {{ ref('int_efi2_scores') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Calculated eFI2 score per living person aged 65+ as at end_date, recomputed from current OLIDS data rather than GP-recorded like efi or clinician-assessed like Rockwood. This is the preferred frailty measure: it covers the whole 65+ cohort (~258k people, against ~10k with a GP-recorded eFI) and reflects the latest data rather than whenever a practice last coded a score. Use for the current eFI2 cohort only; do not combine its category with efi or Rockwood categories. Profiled 2026-08-11: 63.58% Robust, 20.57% Mild, 9.18% Moderate, 6.68% Severe among 258,355 scored people; 4,732 had no matching demographics row.',

    rockwood AS {{ ref('int_rockwood_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest Rockwood Clinical Frailty Scale score (1-9)',

    lft AS {{ ref('int_lft_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest liver function tests (ALT, GGT, bilirubin) with high flags vs clinical upper reference limits. Pairs with NAFLD/chronic liver disease registers.',

    haemoglobin AS {{ ref('int_haemoglobin_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest haemoglobin with anaemia category',

    platelets AS {{ ref('int_platelets_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest platelet count with category',

    eosinophils AS {{ ref('int_eosinophil_count_latest') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Latest blood eosinophil count with category. Relevant to asthma/COPD phenotyping and biologic eligibility.'
)

RELATIONSHIPS(
    bp (person_id) REFERENCES demographics,
    bp_control (person_id) REFERENCES demographics,
    hba1c (person_id) REFERENCES demographics,
    cholesterol (person_id) REFERENCES demographics,
    ldl (person_id) REFERENCES demographics,
    bmi (person_id) REFERENCES demographics,
    waist (person_id) REFERENCES demographics,
    egfr (person_id) REFERENCES demographics,
    creatinine (person_id) REFERENCES demographics,
    qrisk (person_id) REFERENCES demographics,
    acr (person_id) REFERENCES demographics,
    efi (person_id) REFERENCES demographics,
    efi2 (person_id) REFERENCES demographics,
    rockwood (person_id) REFERENCES demographics,
    lft (person_id) REFERENCES demographics,
    haemoglobin (person_id) REFERENCES demographics,
    platelets (person_id) REFERENCES demographics,
    eosinophils (person_id) REFERENCES demographics
)

FACTS(
    -- Cardiovascular
    bp.systolic_value AS systolic_value COMMENT = 'Systolic BP (mmHg)',
    bp.diastolic_value AS diastolic_value COMMENT = 'Diastolic BP (mmHg)',
    cholesterol.cholesterol_value AS cholesterol_value COMMENT = 'Total cholesterol (mmol/L)',
    ldl.ldl_cholesterol_value AS cholesterol_value COMMENT = 'LDL cholesterol (mmol/L)',
    qrisk.qrisk_score AS qrisk_score WITH SYNONYMS = ('CVD risk', 'cardiovascular risk') COMMENT = 'QRISK score (%)',
    bp_control.latest_bp_reading_age_months AS latest_bp_reading_age_months COMMENT = 'Months since last BP reading',

    -- Metabolic
    hba1c.hba1c_ifcc AS hba1c_ifcc COMMENT = 'HbA1c value (mmol/mol IFCC)',
    bmi.bmi_value AS bmi_value COMMENT = 'BMI value (kg/m2)',
    waist.waist_circumference_value AS waist_circumference_value COMMENT = 'Waist circumference (cm)',

    -- Renal
    egfr.egfr_value AS egfr_value COMMENT = 'eGFR value (mL/min/1.73m2)',
    creatinine.creatinine_value AS creatinine_value COMMENT = 'Serum creatinine (umol/L)',
    acr.acr_value AS acr_value COMMENT = 'Urine ACR (mg/mmol)',

    -- Liver function
    lft.alt_value AS alt_value WITH SYNONYMS = ('ALT', 'alanine aminotransferase') COMMENT = 'Latest ALT (U/L)',
    lft.ggt_value AS ggt_value WITH SYNONYMS = ('GGT', 'gamma GT') COMMENT = 'Latest GGT (U/L)',
    lft.bilirubin_value AS bilirubin_value COMMENT = 'Latest total bilirubin (umol/L)',

    -- Haematology
    haemoglobin.haemoglobin_value AS inferred_value WITH SYNONYMS = ('Hb', 'haemoglobin') COMMENT = 'Latest haemoglobin (g/L)',
    platelets.platelet_count AS inferred_value WITH SYNONYMS = ('platelets', 'PLT') COMMENT = 'Latest platelet count (10^9/L)',
    eosinophils.eosinophil_count AS inferred_value WITH SYNONYMS = ('eosinophils', 'eos') COMMENT = 'Latest blood eosinophil count (10^9/L)',

    -- Frailty
    efi.latest_efi_score_preferred AS latest_efi_score_preferred COMMENT = 'Latest GP-recorded eFI or coded eFI2 score (0-1). Not the calculated eFI2 score.',
    efi2.efi2_score AS efi_score WITH SYNONYMS = ('calculated eFI2 score', 'electronic frailty index 2 score') COMMENT = 'Calculated electronic Frailty Index 2 score (0-1) for living people aged 65+. Recomputed as at efi2_score_date; use efi2_category for population counts. Do not combine with GP-recorded eFI/eFI2 or Rockwood scores.',
    rockwood.rockwood_score AS rockwood_score COMMENT = 'Rockwood Clinical Frailty Scale score (1-9)',

    -- ESP
    demographics.esp_weight AS esp_weight COMMENT = 'ESP 2013 population weight for this persons age band (out of 100,000 total). Use with age_band_esp for age-standardised rate calculation.',
    demographics.esp_proportion AS esp_proportion COMMENT = 'ESP 2013 weight as proportion (esp_weight / 100,000). Multiply stratum-specific rate by this and SUM across bands to get the ASR.'
)

DIMENSIONS(
    -- Person linkage key
    demographics.person_id AS person_id COMMENT = 'Pseudonymised person key, shared by all sem_olids_* views. Exposed only for cross-view cohort intersection: join CTEs over two views on person_id, then aggregate. Never return person_id in final results.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for linkage to non-OLIDS views (SUS acute activity, cost index, resource index). Every active person has one; the underlying person-patient mapping can be many-to-many, so joins remain approximate at the margins. Join CTEs on sk_patient_id, then aggregate; never return sk_patient_id in final results.',

    -- Observation Dates (each table's latest measurement date)
    bp.latest_bp_date AS clinical_effective_date COMMENT = 'Date of latest BP reading',
    hba1c.latest_hba1c_date AS clinical_effective_date COMMENT = 'Date of latest HbA1c',
    cholesterol.latest_cholesterol_date AS clinical_effective_date COMMENT = 'Date of latest cholesterol',
    ldl.latest_ldl_date AS clinical_effective_date COMMENT = 'Date of latest LDL',
    bmi.latest_bmi_date AS clinical_effective_date COMMENT = 'Date of latest BMI',
    waist.latest_waist_date AS clinical_effective_date COMMENT = 'Date of latest waist circumference',
    egfr.latest_egfr_date AS clinical_effective_date COMMENT = 'Date of latest eGFR',
    creatinine.latest_creatinine_date AS clinical_effective_date COMMENT = 'Date of latest creatinine',
    qrisk.latest_qrisk_date AS clinical_effective_date COMMENT = 'Date of latest QRISK',
    acr.latest_acr_date AS clinical_effective_date COMMENT = 'Date of latest ACR',
    efi.latest_efi_date AS latest_efi_date COMMENT = 'Date of latest GP-recorded eFI or coded eFI2 assessment; not the calculated eFI2 score date.',
    efi2.efi2_score_date AS end_date COMMENT = 'As-at date for the calculated eFI2 score. This is the model refresh date, not a clinical assessment date.',
    rockwood.latest_rockwood_date AS clinical_effective_date COMMENT = 'Date of latest Rockwood assessment',
    lft.last_lft_date AS last_lft_date COMMENT = 'Date of latest liver function test (most recent of ALT/GGT/bilirubin)',
    haemoglobin.latest_haemoglobin_date AS clinical_effective_date COMMENT = 'Date of latest haemoglobin',
    platelets.latest_platelets_date AS clinical_effective_date COMMENT = 'Date of latest platelet count',
    eosinophils.latest_eosinophil_date AS clinical_effective_date COMMENT = 'Date of latest eosinophil count',

    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age AS age COMMENT = 'Current age in years',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands (0-4, 5-9, ..., 80-84, 85+, Unknown)',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands (0-9, 10-19, ..., 70-79, 80+, Unknown)',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS Digital standard age bands (0-4, 5-14, 15-24, ..., 75-84, 85+)',
    demographics.age_band_esp AS age_band_esp COMMENT = 'ESP 2013 age bands (<1, 1-4, 5-9, ..., 80-84, 85-89, 90-94, 95+). Join to esp_weight for standardised rates.',
    demographics.age_life_stage AS age_life_stage COMMENT = 'Life stage (Infant, Toddler, Child, Adolescent, Young Adult, Adult, Older Adult, Elderly, Very Elderly, Unknown)',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (White: British, White: Irish, White: Roma, White: Traveller, White: Other White, Mixed: White and Black Caribbean, Mixed: White and Black African, Mixed: White and Asian, Mixed: Other Mixed, Asian: Indian, Asian: Pakistani, Asian: Bangladeshi, Asian: Chinese, Asian: Other Asian, Black: African, Black: Caribbean, Black: Other Black, Other: Arab, Other: Other, Unknown, Not Stated, Not Recorded, Recorded Not Known, Refused)',
    demographics.ethnicity_granular AS ethnicity_granular COMMENT = 'Detailed ethnicity classification (Unknown if not recorded)',
    demographics.main_language AS main_language COMMENT = 'Main spoken language (Not Recorded if unknown)',
    demographics.interpreter_needed AS interpreter_needed COMMENT = 'Whether interpreter is required',
    demographics.is_active AS is_active COMMENT = 'Currently registered',
    demographics.is_deceased AS is_deceased COMMENT = 'Deceased status',

    -- Organisation
    demographics.registered_practice_code AS practice_code WITH SYNONYMS = ('practice code', 'ODS code', 'GP practice') COMMENT = 'ODS code of the patient''s registered GP practice',
    demographics.registered_practice_name AS practice_name COMMENT = 'Name of the patient''s registered GP practice',
    demographics.registered_pcn_code AS pcn_code COMMENT = 'PCN code of the registered practice',
    demographics.registered_pcn_name AS pcn_name WITH SYNONYMS = ('PCN', 'primary care network') COMMENT = 'PCN name of the registered practice',
    demographics.registered_pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'Registered PCN name with borough prefix',
    demographics.borough_registered AS borough_registered COMMENT = 'Registration borough',
    demographics.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB / place-based partnership ODS code of the registered practice: 93C = NHS North Central London (Camden, Islington, Barnet, Enfield, Haringey); W2U3Z = NHS North West London (Brent, Ealing, Hammersmith and Fulham, Harrow, Hillingdon, Hounslow, Kensington and Chelsea, Westminster). NULL outside the WNL footprint.',
    demographics.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB display name (NHS North Central London or NHS North West London) of the registered practice. NULL outside the WNL footprint.',
    demographics.neighbourhood_registered AS neighbourhood_registered COMMENT = 'Registration neighbourhood',

    -- Geography (residence)
    demographics.lsoa_code_21 AS lsoa_code_21 COMMENT = 'Lower Super Output Area 2021 code',
    demographics.ward_code AS ward_code COMMENT = 'Electoral ward 2025 code',
    demographics.ward_name AS ward_name COMMENT = 'Electoral ward 2025 name',
    demographics.borough_resident AS borough_resident COMMENT = 'Residence borough',
    demographics.is_london_resident AS is_london_resident COMMENT = 'Resides in Greater London',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Residence neighbourhood',

    -- Deprivation
    demographics.imd_decile_19 AS imd_decile_19 COMMENT = 'IMD 2019 decile (1=most deprived, 10=least). NULL if LSOA not mapped.',
    demographics.imd_quintile_19 AS imd_quintile_19 COMMENT = 'IMD 2019 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least). Preferred over 2019.',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',

    -- Blood Pressure (raw)
    bp.is_home_bp_event AS is_home_bp_event WITH SYNONYMS = ('HBPM', 'home monitoring', 'home BP') COMMENT = 'TRUE when the representative latest paired BP reading is home monitored. This flag exists only in this latest-reading view, not sem_olids_observations_history.',
    bp.is_abpm_bp_event AS is_abpm_bp_event WITH SYNONYMS = ('ABPM', 'ambulatory', '24-hour') COMMENT = 'TRUE when the representative latest paired BP reading is ambulatory monitored.',
    bp.is_hypertensive_range AS is_hypertensive_range COMMENT = 'TRUE when any paired reading on the latest BP date was hypertensive: >=140/90 clinic or >=135/85 home/ABPM. It need not describe the representative lowest-of-day reading.',

    -- Blood Pressure Control
    bp_control.is_overall_bp_controlled AS is_overall_bp_controlled WITH SYNONYMS = ('BP at target', 'BP controlled', 'controlled') COMMENT = 'BP controlled per NICE NG136 — measurement-aware (HBPM/ABPM targets are clinic -5 mmHg)',
    bp_control.is_systolic_controlled AS is_systolic_controlled COMMENT = 'Systolic BP below NG136 target (HBPM/ABPM target is clinic -5 mmHg)',
    bp_control.is_diastolic_controlled AS is_diastolic_controlled COMMENT = 'Diastolic BP below NG136 target (HBPM/ABPM target is clinic -5 mmHg)',
    bp_control.hypertension_stage AS hypertension_stage COMMENT = 'NICE NG136 stage from the higher systolic/diastolic stage: Normal, Stage 1, Stage 2, or Stage 3 (Severe). Home/ABPM uses lower Stage 1/2 thresholds.',
    bp_control.hypertension_stage_number AS hypertension_stage_number COMMENT = 'Hypertension stage number (0-3)',
    bp_control.applied_patient_group AS applied_patient_group WITH SYNONYMS = ('BP threshold group') COMMENT = 'Which threshold applied (AGE_LT_80, AGE_GE_80, T2DM, CKD, CKD_ACR_GE_70)',
    bp_control.applied_measurement_context AS applied_measurement_context WITH SYNONYMS = ('BP threshold context', 'clinic vs home') COMMENT = 'CLINIC or HBPM_ABPM — which NG136 variant was used to score control, matching the latest reading source',
    bp_control.is_case_finding_candidate AS is_case_finding_candidate WITH SYNONYMS = ('BP case finding') COMMENT = 'Elevated BP but not on HTN register',
    bp_control.is_latest_bp_within_recommended_interval AS is_latest_bp_within_recommended_interval WITH SYNONYMS = ('BP timely', 'timely BP') COMMENT = 'BP within recommended interval',
    bp_control.has_t2dm AS has_t2dm COMMENT = 'Has Type 2 diabetes (affects BP threshold)',
    bp_control.has_ckd AS has_ckd COMMENT = 'Has CKD (affects BP threshold)',
    bp_control.is_diagnosed_htn AS is_diagnosed_htn WITH SYNONYMS = ('on HTN register', 'diagnosed hypertension') COMMENT = 'On hypertension register',

    -- HbA1c Categories
    hba1c.hba1c_category AS hba1c_category COMMENT = 'HbA1c clinical band using IFCC mmol/mol. Bands: Normal (<42), Prediabetes (42–47.9, NICE non-diabetic hyperglycaemia), Diabetes - At NICE Target (48–53.9, NICE NG28 target), Diabetes - Elevated (54–58.0, above NICE target but still within QOF DM021 threshold <=58), Diabetes - Above Target (>58–74.9, misses QOF DM021), Diabetes - High Risk (75–85.9), Diabetes - Very High Risk (>=86).',
    hba1c.meets_qof_target AS meets_qof_target WITH SYNONYMS = ('HbA1c controlled', 'at target') COMMENT = 'HbA1c <=58 mmol/mol (QOF target)',
    hba1c.indicates_diabetes AS indicates_diabetes COMMENT = 'HbA1c >=48 mmol/mol (diabetes diagnostic)',

    -- Cholesterol Categories
    cholesterol.cholesterol_category AS cholesterol_category COMMENT = 'Cholesterol category (Desirable, Borderline, High)',
    ldl.LDL_CVD_Target_Met AS LDL_CVD_Target_Met COMMENT = 'Valid LDL <=2 mmol/L (Met, Not Met); no CVD eligibility or reporting period applied',

    -- BMI Categories
    bmi.bmi_category AS bmi_category COMMENT = 'BMI category (Underweight, Normal, Overweight, Obese Class I, Obese Class II, Obese Class III). Uses ethnicity-adjusted thresholds per NICE NG246.',
    bmi.requires_lower_bmi_thresholds AS requires_lower_bmi_thresholds COMMENT = 'Uses lower BMI thresholds for ethnicity',
    bmi.is_valid_bmi AS is_valid_bmi COMMENT = 'BMI in valid range',

    -- Waist Circumference Categories
    waist.waist_risk_category AS waist_risk_category COMMENT = 'Waist circumference risk (Low Risk, Moderate Risk Female, Moderate Risk, High Risk, Very High Risk)',
    waist.is_high_waist_risk AS is_high_waist_risk COMMENT = 'Waist >=88cm',
    waist.is_very_high_waist_risk AS is_very_high_waist_risk COMMENT = 'Waist >=102cm',

    -- eGFR / CKD Staging
    egfr.ckd_stage AS ckd_stage COMMENT = 'CKD stage (1, 2, 3a, 3b, 4, 5)',
    egfr.is_ckd_indicator AS is_ckd_indicator COMMENT = 'eGFR indicates CKD',

    -- Creatinine Categories
    creatinine.creatinine_category AS creatinine_category COMMENT = 'Creatinine category (Normal, Mildly Elevated, Moderately Elevated, Severely Elevated)',
    creatinine.is_elevated_creatinine AS is_elevated_creatinine COMMENT = 'Creatinine >120 umol/L',

    -- QRISK Categories
    qrisk.qrisk_type AS qrisk_type COMMENT = 'QRISK version (QRISK2, QRISK3)',
    qrisk.cvd_risk_category AS cvd_risk_category COMMENT = 'CVD risk category (Low, Moderate, High, Very High)',
    qrisk.is_high_cvd_risk AS is_high_cvd_risk WITH SYNONYMS = ('high risk', 'QRISK >= 10') COMMENT = 'QRISK >=10% (high CVD risk)',
    qrisk.is_very_high_cvd_risk AS is_very_high_cvd_risk COMMENT = 'QRISK >=20% (very high CVD risk)',
    qrisk.warrants_statin_consideration AS warrants_statin_consideration WITH SYNONYMS = ('statin warranted') COMMENT = 'QRISK warrants statin consideration',

    -- Urine ACR Categories
    acr.acr_category AS acr_category COMMENT = 'ACR category (Normal, Moderate, Severe)',
    acr.is_acr_elevated AS is_acr_elevated COMMENT = 'ACR >=3 mg/mmol',
    acr.is_microalbuminuria AS is_microalbuminuria COMMENT = 'Microalbuminuria present',
    acr.is_macroalbuminuria AS is_macroalbuminuria COMMENT = 'Macroalbuminuria present',

    -- Liver Function
    lft.is_high_alt AS is_high_alt COMMENT = 'ALT above clinical upper reference limit',
    lft.is_high_ggt AS is_high_ggt COMMENT = 'GGT above clinical upper reference limit',
    lft.is_high_bilirubin AS is_high_bilirubin COMMENT = 'Bilirubin above clinical upper reference limit',
    lft.high_lft AS high_lft WITH SYNONYMS = ('abnormal LFT', 'deranged LFTs') COMMENT = 'Any of ALT/GGT/bilirubin above its upper reference limit',

    -- Haematology
    haemoglobin.haemoglobin_category AS haemoglobin_category WITH SYNONYMS = ('anaemia category') COMMENT = 'Haemoglobin category (e.g. Anaemia, Normal, High)',
    platelets.platelets_category AS platelets_category WITH SYNONYMS = ('thrombocytopenia category') COMMENT = 'Platelet count category (e.g. Low, Normal, High)',
    eosinophils.eosinophil_category AS eosinophil_category COMMENT = 'Eosinophil count category (e.g. Normal, Raised). Raised eosinophils inform asthma/COPD biologic eligibility.',

    -- Electronic Frailty Index
    efi.latest_efi_type_preferred AS latest_efi_type_preferred COMMENT = 'GP-recorded algorithm type (EFI, EFI2). Uses the most recent record; not the calculated eFI2 model.',
    efi.latest_efi_category_preferred AS latest_efi_category_preferred WITH SYNONYMS = ('recorded eFI category') COMMENT = 'Category of the latest GP-recorded eFI or coded eFI2 (Fit, Mildly Frail, Moderately Frail, Severely Frail). Do not use as the calculated eFI2 category; coverage is incomplete.',
    efi2.efi2_category AS category WITH SYNONYMS = ('calculated frailty category', 'eFI2 category', 'electronic frailty index 2 category') COMMENT = 'Calculated eFI2 category for living people aged 65+: Robust (<0.0857), Mild Frailty (>=0.0857 to <0.1624), Moderate Frailty (>=0.1624 to <=0.2391), Severe Frailty (>0.2391). Use this rather than efi2_score for population health counts. Separate from GP-recorded eFI/eFI2 and Rockwood categories.',

    -- Rockwood Clinical Frailty Scale
    rockwood.frailty_category AS frailty_category WITH SYNONYMS = ('Rockwood category', 'CFS category') COMMENT = 'Rockwood frailty category (Fit, Vulnerable, Mild Frailty, Moderate Frailty, Severe Frailty)',
    rockwood.is_frail AS is_frail COMMENT = 'Rockwood score >=5 (frail)',
    rockwood.is_severely_frail AS is_severely_frail COMMENT = 'Rockwood score >=7 (severely frail)'
)

METRICS(
    -- Population
    demographics.patient_count AS COUNT(DISTINCT demographics.person_id) COMMENT = 'Total patients',

    -- Blood Pressure
    bp.patients_with_bp AS COUNT(DISTINCT bp.person_id) COMMENT = 'Patients with BP measurement',
    bp_control.patients_with_bp_assessment AS COUNT(DISTINCT bp_control.person_id) COMMENT = 'Patients with BP control assessment',
    bp_control.bp_controlled_count AS COUNT(DISTINCT CASE WHEN bp_control.is_overall_bp_controlled THEN bp_control.person_id END) COMMENT = 'Patients with BP controlled',
    bp_control.bp_uncontrolled_count AS COUNT(DISTINCT CASE WHEN NOT bp_control.is_overall_bp_controlled THEN bp_control.person_id END) COMMENT = 'Patients with BP uncontrolled',
    bp_control.bp_stage_1_plus_count AS COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number >= 1 THEN bp_control.person_id END) COMMENT = 'Patients with Stage 1+ HTN',
    bp_control.bp_stage_2_plus_count AS COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number >= 2 THEN bp_control.person_id END) COMMENT = 'Patients with Stage 2+ HTN',
    bp_control.bp_stage_3_count AS COUNT(DISTINCT CASE WHEN bp_control.hypertension_stage_number = 3 THEN bp_control.person_id END) COMMENT = 'Patients with Stage 3 (severe) HTN',
    bp_control.bp_case_finding_count AS COUNT(DISTINCT CASE WHEN bp_control.is_case_finding_candidate THEN bp_control.person_id END) COMMENT = 'BP case finding candidates',
    bp_control.bp_timely_count AS COUNT(DISTINCT CASE WHEN bp_control.is_latest_bp_within_recommended_interval THEN bp_control.person_id END) COMMENT = 'Patients with timely BP',

    -- HbA1c
    hba1c.patients_with_hba1c AS COUNT(DISTINCT hba1c.person_id) COMMENT = 'Patients with HbA1c',
    hba1c.hba1c_at_target_count AS COUNT(DISTINCT CASE WHEN hba1c.meets_qof_target THEN hba1c.person_id END) COMMENT = 'Patients with HbA1c at QOF target',
    hba1c.hba1c_above_target_count AS COUNT(DISTINCT CASE WHEN NOT hba1c.meets_qof_target THEN hba1c.person_id END) COMMENT = 'Patients with HbA1c above QOF target',
    hba1c.hba1c_high_risk_count AS COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Diabetes - High Risk' THEN hba1c.person_id END) COMMENT = 'Patients with HbA1c 75-85 (high risk)',
    hba1c.hba1c_very_high_risk_count AS COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Diabetes - Very High Risk' THEN hba1c.person_id END) COMMENT = 'Patients with HbA1c >=86 (very high risk)',
    hba1c.hba1c_poor_control_count AS COUNT(DISTINCT CASE WHEN hba1c.hba1c_category IN ('Diabetes - High Risk', 'Diabetes - Very High Risk') THEN hba1c.person_id END) COMMENT = 'Patients with HbA1c >=75 (poor control)',
    hba1c.hba1c_prediabetes_count AS COUNT(DISTINCT CASE WHEN hba1c.hba1c_category = 'Prediabetes' THEN hba1c.person_id END) COMMENT = 'Patients with prediabetes HbA1c',

    -- Cholesterol
    cholesterol.patients_with_cholesterol AS COUNT(DISTINCT cholesterol.person_id) COMMENT = 'Patients with cholesterol',
    cholesterol.cholesterol_desirable_count AS COUNT(DISTINCT CASE WHEN cholesterol.cholesterol_category = 'Desirable' THEN cholesterol.person_id END) COMMENT = 'Patients with desirable cholesterol',
    cholesterol.cholesterol_high_count AS COUNT(DISTINCT CASE WHEN cholesterol.cholesterol_category = 'High' THEN cholesterol.person_id END) COMMENT = 'Patients with high cholesterol',
    ldl.patients_with_ldl AS COUNT(DISTINCT ldl.person_id) COMMENT = 'Patients with LDL',
    ldl.ldl_at_target_count AS COUNT(DISTINCT CASE WHEN ldl.LDL_CVD_Target_Met = 'Met' THEN ldl.person_id END) COMMENT = 'People with latest valid LDL <=2 mmol/L; no CVD eligibility or reporting period applied',

    -- BMI
    bmi.patients_with_bmi AS COUNT(DISTINCT bmi.person_id) COMMENT = 'Patients with BMI',
    bmi.bmi_normal_count AS COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Normal' THEN bmi.person_id END) COMMENT = 'Patients with normal BMI',
    bmi.bmi_overweight_count AS COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Overweight' THEN bmi.person_id END) COMMENT = 'Patients overweight',
    bmi.bmi_obese_count AS COUNT(DISTINCT CASE WHEN bmi.bmi_category IN ('Obese Class I', 'Obese Class II', 'Obese Class III') THEN bmi.person_id END) COMMENT = 'Patients with obesity',
    bmi.bmi_obese_class_3_count AS COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Obese Class III' THEN bmi.person_id END) COMMENT = 'Patients with severe obesity',
    bmi.bmi_underweight_count AS COUNT(DISTINCT CASE WHEN bmi.bmi_category = 'Underweight' THEN bmi.person_id END) COMMENT = 'Patients underweight',

    -- Waist Circumference
    waist.patients_with_waist AS COUNT(DISTINCT waist.person_id) COMMENT = 'Patients with waist measurement',
    waist.waist_high_risk_count AS COUNT(DISTINCT CASE WHEN waist.is_high_waist_risk THEN waist.person_id END) COMMENT = 'Patients with high waist risk (>=88cm)',
    waist.waist_very_high_risk_count AS COUNT(DISTINCT CASE WHEN waist.is_very_high_waist_risk THEN waist.person_id END) COMMENT = 'Patients with very high waist risk (>=102cm)',

    -- eGFR / CKD
    egfr.patients_with_egfr AS COUNT(DISTINCT egfr.person_id) COMMENT = 'Patients with eGFR',
    egfr.ckd_indicator_count AS COUNT(DISTINCT CASE WHEN egfr.is_ckd_indicator THEN egfr.person_id END) COMMENT = 'Patients with CKD indicator',
    egfr.ckd_stage_3_plus_count AS COUNT(DISTINCT CASE WHEN egfr.ckd_stage IN ('3a', '3b', '4', '5') THEN egfr.person_id END) COMMENT = 'Patients with CKD Stage 3+',
    egfr.ckd_stage_4_plus_count AS COUNT(DISTINCT CASE WHEN egfr.ckd_stage IN ('4', '5') THEN egfr.person_id END) COMMENT = 'Patients with CKD Stage 4-5',

    -- Creatinine
    creatinine.patients_with_creatinine AS COUNT(DISTINCT creatinine.person_id) COMMENT = 'Patients with creatinine',
    creatinine.creatinine_elevated_count AS COUNT(DISTINCT CASE WHEN creatinine.is_elevated_creatinine THEN creatinine.person_id END) COMMENT = 'Patients with elevated creatinine',

    -- QRISK
    qrisk.patients_with_qrisk AS COUNT(DISTINCT qrisk.person_id) COMMENT = 'Patients with QRISK',
    qrisk.qrisk_high_risk_count AS COUNT(DISTINCT CASE WHEN qrisk.is_high_cvd_risk THEN qrisk.person_id END) COMMENT = 'Patients with QRISK >=10%',
    qrisk.qrisk_very_high_risk_count AS COUNT(DISTINCT CASE WHEN qrisk.is_very_high_cvd_risk THEN qrisk.person_id END) COMMENT = 'Patients with QRISK >=20%',
    qrisk.qrisk_statin_count AS COUNT(DISTINCT CASE WHEN qrisk.warrants_statin_consideration THEN qrisk.person_id END) COMMENT = 'Patients where statin warranted',

    -- Urine ACR
    acr.patients_with_acr AS COUNT(DISTINCT acr.person_id) COMMENT = 'Patients with ACR',
    acr.acr_elevated_count AS COUNT(DISTINCT CASE WHEN acr.is_acr_elevated THEN acr.person_id END) COMMENT = 'Patients with elevated ACR',
    acr.microalbuminuria_count AS COUNT(DISTINCT CASE WHEN acr.is_microalbuminuria THEN acr.person_id END) COMMENT = 'Patients with microalbuminuria',
    acr.macroalbuminuria_count AS COUNT(DISTINCT CASE WHEN acr.is_macroalbuminuria THEN acr.person_id END) COMMENT = 'Patients with macroalbuminuria',

    -- Frailty (eFI)
    efi.patients_with_efi AS COUNT(DISTINCT efi.person_id) COMMENT = 'Patients with eFI assessment',
    efi.efi_mildly_frail_count AS COUNT(DISTINCT CASE WHEN efi.latest_efi_category_preferred = 'Mildly Frail' THEN efi.person_id END) COMMENT = 'Patients mildly frail (eFI)',
    efi.efi_moderately_frail_count AS COUNT(DISTINCT CASE WHEN efi.latest_efi_category_preferred = 'Moderately Frail' THEN efi.person_id END) COMMENT = 'Patients moderately frail (eFI)',
    efi.efi_severely_frail_count AS COUNT(DISTINCT CASE WHEN efi.latest_efi_category_preferred = 'Severely Frail' THEN efi.person_id END) COMMENT = 'Patients severely frail (eFI)',

    -- Frailty (calculated eFI2)
    efi2.patients_with_efi2 AS COUNT(DISTINCT efi2.person_id) COMMENT = 'Living people aged 65+ with a calculated eFI2 score',
    efi2.efi2_robust_count AS COUNT(DISTINCT CASE WHEN efi2.category = 'ROBUST' THEN efi2.person_id END) COMMENT = 'People classed Robust by calculated eFI2',
    efi2.efi2_mild_frailty_count AS COUNT(DISTINCT CASE WHEN efi2.category = 'MILD FRAILTY' THEN efi2.person_id END) COMMENT = 'People classed Mild Frailty by calculated eFI2',
    efi2.efi2_moderate_frailty_count AS COUNT(DISTINCT CASE WHEN efi2.category = 'MODERATE FRAILTY' THEN efi2.person_id END) COMMENT = 'People classed Moderate Frailty by calculated eFI2',
    efi2.efi2_severe_frailty_count AS COUNT(DISTINCT CASE WHEN efi2.category = 'SEVERE FRAILTY' THEN efi2.person_id END) COMMENT = 'People classed Severe Frailty by calculated eFI2',

    -- Frailty (Rockwood)
    rockwood.patients_with_rockwood AS COUNT(DISTINCT rockwood.person_id) COMMENT = 'Patients with Rockwood assessment',
    rockwood.rockwood_frail_count AS COUNT(DISTINCT CASE WHEN rockwood.is_frail THEN rockwood.person_id END) COMMENT = 'Patients frail (Rockwood >=5)',
    rockwood.rockwood_severely_frail_count AS COUNT(DISTINCT CASE WHEN rockwood.is_severely_frail THEN rockwood.person_id END) COMMENT = 'Patients severely frail (Rockwood >=7)',

    -- Liver Function
    lft.patients_with_lft AS COUNT(DISTINCT lft.person_id) COMMENT = 'Patients with any liver function test',
    lft.high_lft_count AS COUNT(DISTINCT CASE WHEN lft.high_lft THEN lft.person_id END) COMMENT = 'Patients with any abnormal LFT (ALT/GGT/bilirubin above reference)',

    -- Haematology
    haemoglobin.patients_with_haemoglobin AS COUNT(DISTINCT haemoglobin.person_id) COMMENT = 'Patients with haemoglobin',
    platelets.patients_with_platelets AS COUNT(DISTINCT platelets.person_id) COMMENT = 'Patients with platelet count',
    eosinophils.patients_with_eosinophils AS COUNT(DISTINCT eosinophils.person_id) COMMENT = 'Patients with eosinophil count',

    -- Averages
    bp.avg_systolic_bp AS AVG(bp.systolic_value) COMMENT = 'Average systolic BP',
    bp.avg_diastolic_bp AS AVG(bp.diastolic_value) COMMENT = 'Average diastolic BP',
    hba1c.avg_hba1c AS AVG(hba1c.hba1c_ifcc) COMMENT = 'Average HbA1c',
    cholesterol.avg_cholesterol AS AVG(cholesterol.cholesterol_value) COMMENT = 'Average cholesterol',
    ldl.avg_ldl AS AVG(ldl.cholesterol_value) COMMENT = 'Average LDL',
    bmi.avg_bmi AS AVG(bmi.bmi_value) COMMENT = 'Average BMI',
    egfr.avg_egfr AS AVG(egfr.egfr_value) COMMENT = 'Average eGFR',
    qrisk.avg_qrisk AS AVG(qrisk.qrisk_score) COMMENT = 'Average QRISK',
    acr.avg_acr AS AVG(acr.acr_value) COMMENT = 'Average ACR',
    efi.avg_efi_score AS AVG(efi.latest_efi_score_preferred) COMMENT = 'Average eFI score',
    efi2.avg_efi2_score AS AVG(efi2.efi_score) COMMENT = 'Average calculated eFI2 score among living people aged 65+',
    rockwood.avg_rockwood AS AVG(rockwood.rockwood_score) COMMENT = 'Average Rockwood score',
    lft.avg_alt AS AVG(lft.alt_value) COMMENT = 'Average ALT (U/L)',
    lft.avg_ggt AS AVG(lft.ggt_value) COMMENT = 'Average GGT (U/L)',
    haemoglobin.avg_haemoglobin AS AVG(haemoglobin.inferred_value) COMMENT = 'Average haemoglobin (g/L)',
    platelets.avg_platelets AS AVG(platelets.inferred_value) COMMENT = 'Average platelet count (10^9/L)',
    eosinophils.avg_eosinophils AS AVG(eosinophils.inferred_value) COMMENT = 'Average eosinophil count (10^9/L)'
)

COMMENT = 'OLIDS Clinical Observations Semantic View - Latest biomarkers and frailty scores with category-based metrics. Includes patient-specific BP thresholds, liver function (ALT/GGT/bilirubin), haematology (haemoglobin/platelets/eosinophils), calculated eFI2, GP-recorded eFI/eFI2, and Rockwood frailty. Grain: one row per person; calculated eFI2 is current as-at scoring only, not a clinical history. ESP 2013 weights available via age_band_esp. Diabetes care processes, foot exam, and retinal screening live in sem_olids_diabetes_care. NICE BP indicators IND239-246 live in sem_olids_bp_indicators.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE, reduce to one row per person before joining on person_id, then aggregate; keep person_id out of the final output. This is one row per person with latest values; filter is_active = TRUE for current cohorts. Example: SELECT borough_resident, AGG(patients_with_bp_assessment), AGG(bp_controlled_count) FROM SEM_OLIDS_OBSERVATIONS WHERE is_active = TRUE GROUP BY borough_resident. Example linkage: reduce out-of-target HbA1c patients here and SGLT2 exposure in sem_olids_prescribing before joining. BP control here uses patient-specific thresholds (T2DM, CKD, age); use sem_olids_bp_indicators for NICE IND239-246 fixed-target achievement. Prefer category-based counts over averages for population health questions. For eFI2 population questions, use efi2_category; its rows are already limited to the living 65+ scoring cohort. It is a calculated current score, not the GP-recorded eFI/eFI2 fields or Rockwood. Diabetes care processes and retinal screening are in sem_olids_diabetes_care. age_band_esp and esp_proportion are ESP 2013 age-only weights for standardised rates.'
AI_QUESTION_CATEGORIZATION 'Use this view for questions about: general patient-specific BP control, HbA1c control, cholesterol, BMI, waist circumference, eGFR, CKD staging, creatinine, QRISK, ACR, liver function (ALT, GGT, bilirubin, abnormal LFTs), haemoglobin/anaemia, platelets, eosinophils, and current frailty. For NICE BP indicators IND239-246 use sem_olids_bp_indicators. Use efi2_category for calculated eFI2 population counts in living people aged 65+; use latest_efi_* only for GP-recorded eFI/eFI2 and frailty_category only for Rockwood assessments. This view holds the LATEST value per biomarker only; calculated eFI2 has no serial history. For serial clinical readings, latest-2, or trajectory over time use sem_olids_observations_history. For condition prevalence and demographics use sem_olids_population. For condition trends over time use sem_olids_trends. Questions needing cohorts from TWO domains (e.g. biomarker control x medication, care-process gaps x appointment access) are answerable by joining this view to the other sem_olids_* views on person_id in CTEs, with aggregate-only output.'
