{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    SUS Acute Activity Semantic View
    ================================

    APC is admission-spell grain, UEC is A&E-attendance grain, and OP is
    attended-outpatient-appointment grain. Do not mix settings in one query.
    Cost is the SUS listed/national price, not actual provider cost; use
    sem_cost_index for SLAM actual provider cost.

#}

TABLES(
    apc AS {{ ref('obt_encounter_apc') }}
        PRIMARY KEY (visit_occurrence_id)
        COMMENT = 'Admitted patient care admission spells',

    uec AS {{ ref('obt_encounter_uec') }}
        PRIMARY KEY (visit_occurrence_id)
        COMMENT = 'Urgent and emergency care attendances',

    op AS {{ ref('obt_encounter_op') }}
        PRIMARY KEY (visit_occurrence_id)
        COMMENT = 'Attended outpatient appointments'
)

FACTS(
    apc.apc_length_of_stay_days AS duration COMMENT = 'Admission spell length of stay in days',
    apc.apc_length_of_stay_to_date_days AS duration_to_date COMMENT = 'Length of stay in days including ongoing stays to current date',
    uec.uec_duration_minutes AS duration COMMENT = 'Urgent and emergency care attendance duration in minutes'
)

DIMENSIONS(
    -- Cross-dataset linkage keys
    apc.apc_sk_patient_id AS sk_patient_id COMMENT = 'Pseudonymised patient key for cross-dataset linkage to sem_olids_population.sk_patient_id. Join CTEs on sk_patient_id, then aggregate. Never return sk_patient_id in final results.',
    uec.uec_sk_patient_id AS sk_patient_id COMMENT = 'Pseudonymised patient key for cross-dataset linkage to sem_olids_population.sk_patient_id. Join CTEs on sk_patient_id, then aggregate. Never return sk_patient_id in final results.',
    op.op_sk_patient_id AS sk_patient_id COMMENT = 'Pseudonymised patient key for cross-dataset linkage to sem_olids_population.sk_patient_id. Join CTEs on sk_patient_id, then aggregate. Never return sk_patient_id in final results.',

    -- APC admission spells
    apc.apc_start_date AS start_date COMMENT = 'Admission spell start date',
    apc.apc_end_date AS end_date COMMENT = 'Admission spell end date',
    apc.apc_provider_id AS organisation_id COMMENT = 'Provider organisation identifier',
    apc.apc_provider_name AS organisation_name COMMENT = 'Provider organisation name',
    apc.apc_site_id AS site_id COMMENT = 'Provider site identifier',
    apc.apc_site_name AS site_name COMMENT = 'Provider site name',
    apc.apc_pod AS pod COMMENT = 'Point of delivery',
    apc.apc_admission_method_code AS spell_admission_method COMMENT = 'Admission method code',
    apc.apc_admission_method_name AS admission_method_name COMMENT = 'Admission method name',
    apc.apc_admission_method_group AS admission_method_group COMMENT = 'Admission method group',
    apc.apc_patient_classification AS admission_patient_classification COMMENT = 'Admission patient classification',
    apc.apc_main_specialty_code AS main_specialty_code COMMENT = 'Main specialty code',
    apc.apc_main_specialty_name AS main_specialty_name COMMENT = 'Main specialty name',
    apc.apc_main_specialty_category AS main_specialty_category COMMENT = 'Main specialty category',
    apc.apc_treatment_function_code AS treatment_function_code COMMENT = 'Treatment function code',
    apc.apc_treatment_function_description AS treatment_function_code_desc COMMENT = 'Treatment function description',
    apc.apc_hrg_code AS hrg_code COMMENT = 'HRG code',
    apc.apc_hrg_description AS core_hrg_desc COMMENT = 'HRG description',
    apc.apc_hrg_chapter AS core_hrg_chapter COMMENT = 'HRG chapter',
    apc.apc_hrg_chapter_description AS core_hrg_chapter_desc COMMENT = 'HRG chapter description',
    apc.apc_age_at_event AS age_at_event COMMENT = 'Age at admission spell',
    apc.apc_gender_at_event AS gender_desc_at_event COMMENT = 'Gender at admission spell',

    -- UEC attendances
    uec.uec_start_date AS start_date COMMENT = 'Urgent and emergency care arrival date',
    uec.uec_end_date AS end_date COMMENT = 'Urgent and emergency care departure date',
    uec.uec_provider_id AS organisation_id COMMENT = 'Provider organisation identifier',
    uec.uec_provider_name AS organisation_name COMMENT = 'Provider organisation name',
    uec.uec_site_id AS site_id COMMENT = 'Provider site identifier',
    uec.uec_site_name AS site_name COMMENT = 'Provider site name',
    uec.uec_pod AS pod COMMENT = 'Point of delivery',
    uec.uec_department_type AS department_type COMMENT = 'Urgent and emergency care department type',
    uec.uec_arrival_mode AS arrival_mode_desc COMMENT = 'UEC arrival mode: own transport (~72%), emergency road ambulance (~13%), public transport (~10%), or less common ambulance/police/prison/air transport. NULL means not recorded.',
    uec.uec_discharge_destination AS discharge_destination_desc COMMENT = 'UEC discharge destination after the attendance',
    uec.uec_main_specialty_code AS main_specialty_code COMMENT = 'Main specialty code',
    uec.uec_main_specialty_name AS main_specialty_name COMMENT = 'Main specialty name',
    uec.uec_hrg_code AS hrg_code COMMENT = 'HRG code',
    uec.uec_hrg_description AS core_hrg_desc COMMENT = 'HRG description',
    uec.uec_hrg_chapter AS core_hrg_chapter COMMENT = 'HRG chapter',
    uec.uec_hrg_chapter_description AS core_hrg_chapter_desc COMMENT = 'HRG chapter description',
    uec.uec_age_at_event AS age_at_event COMMENT = 'Age at attendance',
    uec.uec_gender_at_event AS gender_desc_at_event COMMENT = 'Gender at attendance',

    -- Outpatient attended appointments
    op.op_start_date AS start_date COMMENT = 'Outpatient appointment date',
    op.op_provider_id AS organisation_id COMMENT = 'Provider organisation identifier',
    op.op_provider_name AS organisation_name COMMENT = 'Provider organisation name',
    op.op_site_id AS site_id COMMENT = 'Provider site identifier',
    op.op_site_name AS site_name COMMENT = 'Provider site name',
    op.op_pod AS pod COMMENT = 'Point of delivery',
    op.op_attendance_category AS appointment_attended_or_dna COMMENT = 'Appointment attendance category',
    op.op_attendance_outcome AS appointment_attendance_outcome_desc COMMENT = 'Appointment attendance outcome',
    op.op_first_attendance AS appointment_first_attendance COMMENT = 'First or follow-up attendance',
    op.op_main_specialty_code AS main_specialty_code COMMENT = 'Main specialty code',
    op.op_main_specialty_name AS main_specialty_name COMMENT = 'Main specialty name',
    op.op_main_specialty_category AS main_specialty_category COMMENT = 'Main specialty category',
    op.op_treatment_function_code AS treatment_function_code COMMENT = 'Treatment function code',
    op.op_treatment_function_description AS treatment_function_code_desc COMMENT = 'Treatment function description',
    op.op_hrg_code AS core_hrg_code COMMENT = 'HRG code',
    op.op_hrg_description AS core_hrg_desc COMMENT = 'HRG description',
    op.op_hrg_chapter AS core_hrg_chapter COMMENT = 'HRG chapter',
    op.op_hrg_chapter_description AS core_hrg_chapter_desc COMMENT = 'HRG chapter description',
    op.op_age_at_event AS age_at_event COMMENT = 'Age at appointment',
    op.op_gender_at_event AS gender_desc_at_event COMMENT = 'Gender at appointment'
)

METRICS(
    apc.apc_spell_count AS COUNT(apc.visit_occurrence_id) COMMENT = 'Admission spells',
    apc.apc_patient_count AS COUNT(DISTINCT apc.sk_patient_id) COMMENT = 'People with admission spells',
    apc.apc_total_cost AS SUM(apc.cost) COMMENT = 'SUS listed/national price, not actual provider cost',
    apc.apc_avg_length_of_stay_days AS AVG(apc.duration) COMMENT = 'Average admission spell length of stay in days',
    apc.apc_total_length_of_stay_days AS SUM(apc.duration) COMMENT = 'Total admission spell length of stay in days',
    uec.uec_attendance_count AS COUNT(uec.visit_occurrence_id) COMMENT = 'Urgent and emergency care attendances',
    uec.uec_patient_count AS COUNT(DISTINCT uec.sk_patient_id) COMMENT = 'People with urgent and emergency care attendances',
    uec.uec_total_cost AS SUM(uec.cost) COMMENT = 'SUS listed/national price, not actual provider cost',
    op.op_attendance_count AS COUNT(op.visit_occurrence_id) COMMENT = 'Attended outpatient appointments',
    op.op_patient_count AS COUNT(DISTINCT op.sk_patient_id) COMMENT = 'People with attended outpatient appointments',
    op.op_total_cost AS SUM(op.cost) COMMENT = 'SUS listed/national price, not actual provider cost'
)

COMMENT = 'SUS Acute Activity Semantic View - APC admission spells, UEC A&E attendances, and attended outpatient appointments. Cost is SUS listed/national price, not actual provider cost. Do not mix settings in one query. Coverage: UEC and OP from 2018-04; APC reaches back to the 1990s but validate density before long trends. Planned APC admissions can carry a future apc_start_date - filter apc_start_date <= CURRENT_DATE for activity to date.'
AI_SQL_GENERATION 'APC is admission-spell grain, UEC is A&E-attendance grain, and OP is attended outpatient-appointment grain; never mix settings. For cross-dataset linkage, query each semantic view in its own CTE, reduce each CTE to one row per sk_patient_id or aligned period before joining, then aggregate. Use COUNT(DISTINCT sk_patient_id) for people and the view metric for events; keep sk_patient_id out of final output. Only ~30% of UEC people and ~32% of UEC attendances link to an active GP registration, so linked cohorts are incomplete. Example: SELECT apc_admission_method_group, AGG(apc_patient_count), AGG(apc_spell_count) FROM SEM_SUS_ACUTE_ACTIVITY WHERE apc_admission_method_group = ''Non-elective - emergency'' GROUP BY apc_admission_method_group. Example linkage: reduce an active population cohort in sem_olids_population and APC events here before joining on sk_patient_id. Use apc_start_date for APC date windows. Cost is the SUS listed/national price; use sem_cost_index for SLAM actual provider cost.'
AI_QUESTION_CATEGORIZATION 'Use this view for: emergency admissions, A&E attendances, outpatient attendances, acute activity and SUS listed cost by provider or specialty, and acute utilisation of OLIDS-defined cohorts via sk_patient_id joins to sem_olids_population.'
