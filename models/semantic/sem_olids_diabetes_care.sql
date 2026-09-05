{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Diabetes Care Semantic View
    =================================

    Care-process completion and treatment-target achievement for the
    diabetes register. Split out of sem_olids_observations so diabetes
    care questions have one clearly-scoped home.

    OLIDS is the One London Integrated Data Set — primary care data from
    system suppliers (currently EMIS Web, with TPP to follow), unified by
    the One London team.

    Grain: one row per person on the diabetes register (QOF, age >=17).

    Content:
    - 8 care processes (HbA1c, BP, cholesterol, creatinine, urine ACR,
      foot exam, BMI, smoking) + retinal screening = 9 care processes
    - Triple target (HbA1c <=58, BP <130/80, cholesterol <5)
    - Foot check detail (Townson risk, per-foot status, exemptions)

    12-month completion windows are relative to the build date, not a
    fixed QOF year end. Register includes inactive/deceased persons —
    filter is_active = TRUE.
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics, geography, ethnicity, deprivation (current snapshot)',

    cp AS {{ ref('fct_person_diabetes_9_care_processes') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Diabetes 8 + 9 care-process completion (rolling 12 months from build date). One row per person on the diabetes register.',

    triple AS {{ ref('fct_person_diabetes_triple_target') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Triple target achievement: HbA1c <=58 mmol/mol, BP <130/80, total cholesterol <5 mmol/L. Targets use latest-ever values; recency flags are separate.',

    foot AS {{ ref('fct_person_diabetes_foot_check') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Foot check detail: Townson risk level, per-foot status, exemptions, declines'
)

RELATIONSHIPS(
    cp (person_id) REFERENCES demographics,
    triple (person_id) REFERENCES demographics,
    foot (person_id) REFERENCES demographics
)

FACTS(
    cp.care_processes_8_completed AS care_processes_8_completed COMMENT = 'Count of the 8 standard care processes completed in the last 12 months (0-8)',
    cp.care_processes_9_completed AS care_processes_9_completed COMMENT = 'Count including retinal screening (0-9)',
    triple.latest_hba1c_value AS latest_hba1c_value COMMENT = 'Latest HbA1c value used for triple-target assessment, in the original recorded unit. Filter or group by hba1c_unit before summarising; do not apply mmol/mol thresholds across mixed units.',
    triple.latest_systolic AS latest_systolic COMMENT = 'Latest systolic BP in mmHg used for triple-target assessment',
    triple.latest_diastolic AS latest_diastolic COMMENT = 'Latest diastolic BP in mmHg used for triple-target assessment',
    triple.latest_chol_value AS latest_chol_value COMMENT = 'Latest total cholesterol in mmol/L used for triple-target assessment'
)

DIMENSIONS(
    -- Person linkage key (on cp so it can be selected alongside the process-count facts)
    cp.person_id AS person_id COMMENT = 'Pseudonymised person key, shared by all sem_olids_* views. Exposed only for cross-view cohort intersection: join CTEs over two views on person_id, then aggregate. Never return person_id in final results.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for linkage to non-OLIDS views (SUS acute activity, cost index, resource index). Every active person has one; the underlying person-patient mapping can be many-to-many, so joins remain approximate at the margins. Join CTEs on sk_patient_id, then aggregate; never return sk_patient_id in final results.',

    -- Care processes (rolling 12 months from build date)
    cp.hba1c_completed_in_last_12m AS hba1c_completed_in_last_12m WITH SYNONYMS = ('DM HbA1c check') COMMENT = 'HbA1c recorded in last 12m',
    cp.bp_completed_in_last_12m AS bp_completed_in_last_12m WITH SYNONYMS = ('DM BP check') COMMENT = 'BP recorded in last 12m',
    cp.cholesterol_completed_in_last_12m AS cholesterol_completed_in_last_12m WITH SYNONYMS = ('DM cholesterol check') COMMENT = 'Cholesterol recorded in last 12m',
    cp.creatinine_completed_in_last_12m AS creatinine_completed_in_last_12m WITH SYNONYMS = ('DM creatinine check') COMMENT = 'Serum creatinine recorded in last 12m',
    cp.acr_completed_in_last_12m AS acr_completed_in_last_12m WITH SYNONYMS = ('DM ACR check') COMMENT = 'Urine ACR recorded in last 12m',
    cp.foot_check_completed_in_last_12m AS foot_check_completed_in_last_12m WITH SYNONYMS = ('DM foot check') COMMENT = 'Valid foot examination in last 12m (both feet, or one where the other is absent/amputated; not declined/unsuitable)',
    cp.bmi_completed_in_last_12m AS bmi_completed_in_last_12m WITH SYNONYMS = ('DM BMI check') COMMENT = 'BMI recorded in last 12m',
    cp.smoking_completed_in_last_12m AS smoking_completed_in_last_12m WITH SYNONYMS = ('DM smoking check') COMMENT = 'Smoking status recorded in last 12m',
    cp.retinal_screening_completed_in_last_12m AS retinal_screening_completed_in_last_12m WITH SYNONYMS = ('DM eye screening') COMMENT = 'Retinal screening completed in last 12m (9th care process)',
    cp.all_8_processes_completed AS all_8_processes_completed WITH SYNONYMS = ('all 8 care processes', 'DM 8CP') COMMENT = 'All 8 standard care processes completed in last 12m',
    cp.all_9_processes_completed AS all_9_processes_completed WITH SYNONYMS = ('all 9 care processes', 'DM 9CP') COMMENT = 'All 9 care processes (incl. retinal screening) completed in last 12m',
    triple.latest_hba1c_date AS latest_hba1c_date COMMENT = 'Date of the latest HbA1c value used for triple-target assessment',
    cp.latest_retinal_screening_date AS latest_retinal_screening_date COMMENT = 'Date of latest retinal screening',

    -- Triple target
    triple.diabetes_type AS diabetes_type COMMENT = 'Diabetes type (Type 1, Type 2, Unknown)',
    triple.hba1c_in_target_range AS hba1c_in_target_range COMMENT = 'Latest HbA1c <=58 mmol/mol (FALSE if never measured)',
    triple.bp_in_target_range AS bp_in_target_range COMMENT = 'Latest BP <130/80 (NICE diabetes target; FALSE if never measured)',
    triple.cholesterol_in_target_range AS cholesterol_in_target_range COMMENT = 'Latest total cholesterol <5 mmol/L (FALSE if never measured)',
    triple.all_three_targets_met AS all_three_targets_met WITH SYNONYMS = ('triple target') COMMENT = 'All three treatment targets met (latest-ever values)',
    triple.hba1c_unit AS hba1c_unit COMMENT = 'Unit of the latest HbA1c value used for triple-target assessment',
    triple.latest_bp_date AS latest_bp_date COMMENT = 'Date of the latest paired BP used for triple-target assessment',
    triple.latest_chol_date AS latest_chol_date COMMENT = 'Date of the latest total cholesterol used for triple-target assessment',
    triple.hba1c_clinical_category AS hba1c_clinical_category COMMENT = 'HbA1c band: Normal, Prediabetes, Diabetes - At NICE Target, Diabetes - Elevated (within QOF), Diabetes - Above Target, Diabetes - High Risk, or Diabetes - Very High Risk.',
    triple.hba1c_measured_in_last_12m AS hba1c_measured_in_last_12m COMMENT = 'HbA1c recorded in last 12m (triple-target model)',
    triple.bp_measured_in_last_12m AS bp_measured_in_last_12m COMMENT = 'Paired BP used for triple-target assessment was recorded in the last 12 months',
    triple.cholesterol_measured_in_last_12m AS cholesterol_measured_in_last_12m COMMENT = 'Total cholesterol used for triple-target assessment was recorded in the last 12 months',
    triple.hba1c_recent_but_out_of_range AS hba1c_recent_but_out_of_range COMMENT = 'HbA1c measured in 12m but target not met',
    triple.bp_recent_but_out_of_range AS bp_recent_but_out_of_range COMMENT = 'BP measured in 12m but target not met',
    triple.cholesterol_recent_but_out_of_range AS cholesterol_recent_but_out_of_range COMMENT = 'Cholesterol measured in 12m but target not met',

    -- Foot check detail
    foot.foot_check_status AS foot_check_status COMMENT = 'Exemption-first foot check status (Complete - Both Feet, Permanently Exempt - Both Feet Missing, Not Appropriate - Declined/Unsuitable, Partial - Left/Right Only, Not Done)',
    foot.townson_scale_level AS townson_scale_level WITH SYNONYMS = ('foot risk level') COMMENT = 'Recorded Townson foot-risk level. Use for risk detail; use foot_check_completed_in_last_12m for the care-process measure.',
    foot.left_foot_status AS left_foot_status COMMENT = 'Left foot status (risk level, Amputated, Absent (Congenital), Not Assessed)',
    foot.right_foot_status AS right_foot_status COMMENT = 'Right foot status',
    foot.is_permanently_exempt AS is_permanently_exempt COMMENT = 'Both feet absent or amputated — permanently exempt from foot checks',
    foot.latest_foot_check_date AS latest_foot_check_date COMMENT = 'Date of latest foot examination event',

    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS Digital standard age bands',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (detailed groupings)',
    demographics.main_language AS main_language COMMENT = 'Main spoken language (Not Recorded if unknown)',
    demographics.interpreter_needed AS interpreter_needed COMMENT = 'Whether interpreter is required',
    demographics.is_active AS is_active COMMENT = 'Currently registered. Register includes inactive/deceased — filter TRUE.',
    demographics.is_deceased AS is_deceased COMMENT = 'Deceased status',

    -- Organisation (registered practice)
    demographics.registered_practice_code AS practice_code WITH SYNONYMS = ('practice code', 'ODS code', 'GP practice') COMMENT = 'ODS code of the patient''s registered GP practice',
    demographics.registered_practice_name AS practice_name COMMENT = 'Name of the patient''s registered GP practice',
    demographics.registered_pcn_code AS pcn_code COMMENT = 'PCN code of the registered practice',
    demographics.registered_pcn_name AS pcn_name WITH SYNONYMS = ('PCN', 'primary care network') COMMENT = 'PCN name of the registered practice',
    demographics.registered_pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'Registered PCN name with borough prefix',
    demographics.borough_registered AS borough_registered COMMENT = 'Registration borough',
    demographics.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB ODS code of the registered practice: 93C = NHS North Central London; W2U3Z = NHS North West London. NULL outside the WNL footprint.',
    demographics.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB display name. NULL outside the WNL footprint.',
    demographics.neighbourhood_registered AS neighbourhood_registered COMMENT = 'Registration neighbourhood',

    -- Geography and deprivation (residence)
    demographics.borough_resident AS borough_resident COMMENT = 'Residence borough',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Residence neighbourhood',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least)',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)'
)

METRICS(
    -- Denominators
    cp.dm_register_count AS COUNT(DISTINCT cp.person_id) COMMENT = 'People on the diabetes register (care-process model)',
    triple.patients_with_triple_target AS COUNT(DISTINCT triple.person_id) COMMENT = 'People on the diabetes register (triple-target model)',
    foot.patients_with_foot_model AS COUNT(DISTINCT foot.person_id) COMMENT = 'People on the diabetes register (foot-check model)',

    -- Care-process completion
    cp.all_8_count AS COUNT(DISTINCT CASE WHEN cp.all_8_processes_completed THEN cp.person_id END) COMMENT = 'People with all 8 care processes completed in last 12m',
    cp.all_9_count AS COUNT(DISTINCT CASE WHEN cp.all_9_processes_completed THEN cp.person_id END) COMMENT = 'People with all 9 care processes completed in last 12m',
    cp.avg_care_processes_8 AS AVG(cp.care_processes_8_completed) COMMENT = 'Average of the 8 standard care processes completed (0-8)',
    cp.avg_care_processes_9 AS AVG(cp.care_processes_9_completed) COMMENT = 'Average care processes completed including retinal (0-9)',
    cp.hba1c_done_count AS COUNT(DISTINCT CASE WHEN cp.hba1c_completed_in_last_12m THEN cp.person_id END) COMMENT = 'People with HbA1c in last 12m',
    cp.bp_done_count AS COUNT(DISTINCT CASE WHEN cp.bp_completed_in_last_12m THEN cp.person_id END) COMMENT = 'People with BP in last 12m',
    cp.foot_check_done_count AS COUNT(DISTINCT CASE WHEN cp.foot_check_completed_in_last_12m THEN cp.person_id END) COMMENT = 'People with valid foot check in last 12m',
    cp.retinal_done_count AS COUNT(DISTINCT CASE WHEN cp.retinal_screening_completed_in_last_12m THEN cp.person_id END) COMMENT = 'People with retinal screening in last 12m',

    -- Treatment targets
    triple.triple_target_met_count AS COUNT(DISTINCT CASE WHEN triple.all_three_targets_met THEN triple.person_id END) COMMENT = 'People meeting all three treatment targets',
    triple.hba1c_target_met_count AS COUNT(DISTINCT CASE WHEN triple.hba1c_in_target_range THEN triple.person_id END) COMMENT = 'People with HbA1c <=58',
    triple.bp_target_met_count AS COUNT(DISTINCT CASE WHEN triple.bp_in_target_range THEN triple.person_id END) COMMENT = 'People with BP <130/80',
    triple.cholesterol_target_met_count AS COUNT(DISTINCT CASE WHEN triple.cholesterol_in_target_range THEN triple.person_id END) COMMENT = 'People with cholesterol <5',

    -- Foot check
    foot.permanently_exempt_count AS COUNT(DISTINCT CASE WHEN foot.is_permanently_exempt THEN foot.person_id END) COMMENT = 'People permanently exempt from foot checks'
)

COMMENT = 'OLIDS Diabetes Care Semantic View - care-process completion (8 + 9 with retinal), triple treatment targets with their matching latest measurement evidence, and foot-check detail for the diabetes register. Grain: one row per person on the diabetes register (QOF, age >=17). 12-month windows are rolling from build date. Register includes inactive/deceased persons — filter is_active = TRUE. Broader biomarker history and clinical categories live in sem_olids_observations and sem_olids_observations_history.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE, reduce to one row per person before joining on person_id, then aggregate; keep person_id out of the final output. This is person grain; filter is_active = TRUE for current cohorts. Example: SELECT borough_resident, AGG(dm_register_count), AGG(all_8_count) FROM SEM_OLIDS_DIABETES_CARE WHERE is_active = TRUE GROUP BY borough_resident. Example linkage: reduce the diabetes cohort here and appointments in sem_olids_appointments before joining. Completion rate is AGG(all_8_count) / AGG(dm_register_count) (per-process AGG(*_done_count) over AGG(dm_register_count)). Triple-target flags and the latest measurement evidence use the latest-ever values. latest_hba1c_value is in the original recorded hba1c_unit; filter or group by unit before summarising it, and do not apply mmol/mol cut-offs across mixed units. Check the three *_measured_in_last_12m flags before describing target achievement as recent; use *_recent_but_out_of_range for recently measured care gaps. For foot-check care-process reporting prefer the cp foot_check_completed_in_last_12m flag; use the foot model only for risk-level and exemption detail (its recency window uses month-boundary arithmetic and can differ slightly). The 12-month windows are relative to the build date, not the QOF year end.'
AI_QUESTION_CATEGORIZATION 'Use this view for: diabetes 8/9 care processes, care-process gaps, retinal screening, foot checks and foot risk (Townson), triple target status and its matching latest HbA1c/BP/cholesterol evidence, and diabetes care quality by practice/PCN/deprivation/ethnicity. For broader latest biomarker analysis use sem_olids_observations; for serial readings use sem_olids_observations_history. For diabetes prevalence and type use sem_olids_population. For diabetes medications use sem_olids_prescribing.'
