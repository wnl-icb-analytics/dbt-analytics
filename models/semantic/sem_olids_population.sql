{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Population Semantic View
    ==============================

    Semantic model for NCL population health analysis. OLIDS is the One London
    Integrated Data Set — primary care data from system suppliers (currently
    EMIS Web, with TPP to follow), unified by the One London team.

    Combines demographics, conditions, and person status into a unified
    semantic layer for Cortex Analyst and governed metric definitions.

    Grain: One row per person (current state)

    Condition Registers:
    Built to QOF Business Rules v50. QOF registers are marked below.
    Conditions are grouped by clinical domain:
    - Cardiovascular (QOF): AF, CHD, HF, HTN, PAD
    - Respiratory (QOF): Asthma, COPD; (non-QOF): CYP Asthma
    - Mental Health (QOF): Depression, SMI, Dementia; (non-QOF): Anxiety
    - Metabolic (QOF): Diabetes, NDH, CKD, Obesity
    - Renal (QOF): CKD
    - Musculoskeletal (QOF): RA, Osteoporosis; (non-QOF): Osteoarthritis
    - Neurology (QOF): Epilepsy, Stroke/TIA; (non-QOF): Parkinson's, Cerebral Palsy, MND, MS
    - Neurodevelopmental (QOF): Learning Disability; (non-QOF): LD Under 14, Autism, ADHD
    - Oncology (QOF): Cancer
    - Endocrine (non-QOF): Hypothyroidism
    - Hepatology (non-QOF): NAFLD, Chronic Liver Disease
    - Haematology (non-QOF): Sickle Cell Disease, Thalassaemia
    - Genetics (non-QOF): Familial Hypercholesterolaemia
    - Geriatric (non-QOF): Frailty
    - Palliative Care (QOF): Palliative Care
    - Maternity (non-QOF): Gestational Diabetes
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Core patient demographics including registration, geography, and ethnicity. Source: OLIDS (One London Integrated Data Set — primary care data from system suppliers, unified by the One London team).',

    conditions AS {{ ref('dim_person_conditions') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Boolean flags for all long-term conditions (QOF Business Rules v50) with summary counts and diabetes type classification',

    status AS {{ ref('dim_person_status_summary') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Vulnerability factors, polypharmacy, smoking, alcohol, and data sharing status',

    ccms AS {{ ref('dim_person_ccms') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Cambridge Comorbidity Score (2022 modified, 20-category weighted sum). Continuous score only — the CCMS literature does not define risk bands. Only populated for persons aged 16+.'
)

RELATIONSHIPS(
    conditions (person_id) REFERENCES demographics,
    status (person_id) REFERENCES demographics,
    ccms (person_id) REFERENCES demographics
)

FACTS(
    demographics.age AS age COMMENT = 'Current age in years',
    conditions.total_conditions AS total_conditions COMMENT = 'Total number of active conditions',
    conditions.total_qof_conditions AS total_qof_conditions COMMENT = 'Number of QOF-registered conditions',
    conditions.total_non_qof_conditions AS total_non_qof_conditions COMMENT = 'Number of non-QOF conditions',
    conditions.cardiovascular_conditions AS cardiovascular_conditions COMMENT = 'Count of cardiovascular conditions (AF, CHD, HF, HTN, PAD, Stroke/TIA)',
    conditions.respiratory_conditions AS respiratory_conditions COMMENT = 'Count of respiratory conditions (Asthma, COPD)',
    conditions.mental_health_conditions AS mental_health_conditions COMMENT = 'Count of mental health conditions (Depression, SMI, Dementia, Anxiety)',
    conditions.metabolic_conditions AS metabolic_conditions COMMENT = 'Count of metabolic conditions (Diabetes, NDH, CKD, Obesity)',
    conditions.musculoskeletal_conditions AS musculoskeletal_conditions COMMENT = 'Count of musculoskeletal conditions (RA, Osteoporosis, Osteoarthritis)',
    conditions.neurology_conditions AS neurology_conditions COMMENT = 'Count of neurology conditions (Epilepsy, Stroke/TIA, Parkinsons, Cerebral Palsy, MND, MS)',
    conditions.geriatric_conditions AS geriatric_conditions COMMENT = 'Count of geriatric conditions (Frailty)',
    ccms.cambridge_comorbidity_score AS cambridge_comorbidity_score WITH SYNONYMS = ('CCMS', 'comorbidity score', 'Cambridge score') COMMENT = 'Cambridge Comorbidity Score (2022 modified, 20-category weighted sum). Higher = greater comorbidity burden; can be negative. Continuous score per the CCMS literature — no external clinical cut-points exist. Only populated for persons aged 16+.',
    status.medication_count AS medication_count COMMENT = 'Number of current medications',
    status.behavioural_risk_count AS behavioural_risk_count COMMENT = 'Count of behavioural risk factors',
    demographics.esp_weight AS esp_weight COMMENT = 'ESP 2013 population weight for this persons age band (out of 100,000 total). Use with age_band_esp for age-standardised rate calculation.',
    demographics.esp_proportion AS esp_proportion COMMENT = 'ESP 2013 weight as proportion (esp_weight / 100,000). Multiply stratum-specific rate by this and SUM across bands to get the ASR.'
)

DIMENSIONS(
    -- Person linkage key
    demographics.person_id AS person_id COMMENT = 'Pseudonymised person key, shared by all sem_olids_* views. Exposed only for cross-view cohort intersection: join CTEs over two views on person_id, then aggregate. Never return person_id in final results.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for linkage to non-OLIDS views (SUS acute activity, cost index, resource index). Every active person has one; the underlying person-patient mapping can be many-to-many, so joins remain approximate at the margins. Join CTEs on sk_patient_id, then aggregate; never return sk_patient_id in final results.',

    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands (0-4, 5-9, ..., 80-84, 85+, Unknown)',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands (0-9, 10-19, ..., 70-79, 80+, Unknown)',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS Digital standard age bands (0-4, 5-14, 15-24, ..., 75-84, 85+)',
    demographics.age_band_esp AS age_band_esp COMMENT = 'ESP 2013 age bands (<1, 1-4, 5-9, ..., 80-84, 85-89, 90-94, 95+). Join to esp_weight for standardised rates.',
    demographics.age_life_stage AS age_life_stage COMMENT = 'Life stage (Infant, Toddler, Child, Adolescent, Young Adult, Adult, Older Adult, Elderly, Very Elderly, Unknown)',

    -- Ethnicity
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (White: British, White: Irish, White: Roma, White: Traveller, White: Other White, Mixed: White and Black Caribbean, Mixed: White and Black African, Mixed: White and Asian, Mixed: Other Mixed, Asian: Indian, Asian: Pakistani, Asian: Bangladeshi, Asian: Chinese, Asian: Other Asian, Black: African, Black: Caribbean, Black: Other Black, Other: Arab, Other: Other, Unknown, Not Stated, Not Recorded, Recorded Not Known, Refused)',
    demographics.ethnicity_granular AS ethnicity_granular COMMENT = 'Detailed ethnicity classification (Unknown if not recorded)',

    -- Language
    demographics.main_language AS main_language COMMENT = 'Main spoken language (Not Recorded if unknown)',
    demographics.interpreter_needed AS interpreter_needed COMMENT = 'Whether interpreter is required',

    -- Registration Status
    demographics.is_active AS is_active COMMENT = 'Currently registered with NCL GP practice',
    demographics.is_deceased AS is_deceased COMMENT = 'Deceased status',

    -- Practice/Organisation
    demographics.registered_practice_code AS practice_code WITH SYNONYMS = ('practice code', 'ODS code', 'GP practice') COMMENT = 'ODS code of the patient''s registered GP practice',
    demographics.registered_practice_name AS practice_name COMMENT = 'Name of the patient''s registered GP practice',
    demographics.registered_pcn_code AS pcn_code COMMENT = 'PCN code of the registered practice',
    demographics.registered_pcn_name AS pcn_name WITH SYNONYMS = ('PCN', 'primary care network') COMMENT = 'PCN name of the registered practice',
    demographics.registered_pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'Registered PCN name with borough prefix',
    demographics.borough_registered AS borough_registered COMMENT = 'Borough where GP practice is located',
    demographics.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB / place-based partnership ODS code of the registered practice: 93C = NHS North Central London (Camden, Islington, Barnet, Enfield, Haringey); W2U3Z = NHS North West London (Brent, Ealing, Hammersmith and Fulham, Harrow, Hillingdon, Hounslow, Kensington and Chelsea, Westminster). NULL outside the WNL footprint.',
    demographics.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB display name (NHS North Central London or NHS North West London) of the registered practice. NULL outside the WNL footprint.',
    demographics.neighbourhood_registered AS neighbourhood_registered COMMENT = 'NCL neighbourhood based on registration',

    -- Geography (Residence)
    demographics.lsoa_code_21 AS lsoa_code_21 COMMENT = 'Lower Super Output Area 2021 code',
    demographics.ward_code AS ward_code COMMENT = 'Electoral ward 2025 code',
    demographics.ward_name AS ward_name COMMENT = 'Electoral ward 2025 name',
    demographics.borough_resident AS borough_resident COMMENT = 'London borough of residence',
    demographics.is_london_resident AS is_london_resident COMMENT = 'Resides in Greater London',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'NCL neighbourhood based on residence',

    -- Deprivation
    demographics.imd_decile_19 AS imd_decile_19 COMMENT = 'IMD 2019 decile (1=most deprived, 10=least). NULL if LSOA not mapped.',
    demographics.imd_quintile_19 AS imd_quintile_19 COMMENT = 'IMD 2019 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least). Preferred over 2019.',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',

    -- Diabetes Type
    conditions.diabetes_type AS diabetes_type COMMENT = 'Diabetes type classification (Type 1, Type 2, Unknown, Not Diabetic). Type 1 takes precedence if both coded on same date.',

    -- Cardiovascular Conditions (QOF)
    conditions.has_hypertension AS has_hypertension WITH SYNONYMS = ('HTN', 'high blood pressure') COMMENT = 'On hypertension register (QOF, age >=18)',
    conditions.has_coronary_heart_disease AS has_coronary_heart_disease WITH SYNONYMS = ('CHD', 'IHD', 'ischaemic heart disease') COMMENT = 'On CHD register (QOF)',
    conditions.has_heart_failure AS has_heart_failure WITH SYNONYMS = ('HF') COMMENT = 'On heart failure register (QOF)',
    conditions.has_atrial_fibrillation AS has_atrial_fibrillation WITH SYNONYMS = ('AF', 'AFib') COMMENT = 'On AF register (QOF)',
    conditions.has_stroke_tia AS has_stroke_tia WITH SYNONYMS = ('stroke', 'TIA', 'CVA') COMMENT = 'On stroke/TIA register (QOF)',
    conditions.has_peripheral_arterial_disease AS has_peripheral_arterial_disease WITH SYNONYMS = ('PAD', 'PVD') COMMENT = 'On PAD register (QOF)',

    -- Metabolic/Endocrine Conditions
    conditions.has_diabetes AS has_diabetes WITH SYNONYMS = ('DM', 'diabetic', 'T1DM', 'T2DM') COMMENT = 'On diabetes register (QOF, age >=17). Use diabetes_type for T1/T2 split.',
    conditions.has_non_diabetic_hyperglycaemia AS has_non_diabetic_hyperglycaemia WITH SYNONYMS = ('NDH', 'prediabetes') COMMENT = 'Non-diabetic hyperglycaemia (QOF, age >=18)',
    conditions.has_gestational_diabetes AS has_gestational_diabetes COMMENT = 'Gestational diabetes (non-QOF)',
    conditions.has_chronic_kidney_disease AS has_chronic_kidney_disease WITH SYNONYMS = ('CKD', 'renal disease') COMMENT = 'On CKD register (QOF, age >=18)',
    conditions.has_obesity AS has_obesity COMMENT = 'Recorded obesity (QOF, age >=18)',
    conditions.has_hypothyroidism AS has_hypothyroidism WITH SYNONYMS = ('thyroid', 'underactive thyroid') COMMENT = 'Hypothyroidism (non-QOF)',

    -- Respiratory Conditions
    conditions.has_copd AS has_copd WITH SYNONYMS = ('COPD', 'emphysema', 'chronic bronchitis') COMMENT = 'On COPD register (QOF)',
    conditions.has_asthma AS has_asthma COMMENT = 'On asthma register (QOF, age >=6)',
    conditions.has_cyp_asthma AS has_cyp_asthma WITH SYNONYMS = ('children asthma', 'paediatric asthma') COMMENT = 'Children and young people asthma (non-QOF, age 0-17)',

    -- Mental Health Conditions
    conditions.has_depression AS has_depression COMMENT = 'On depression register (QOF, age >=18)',
    conditions.has_severe_mental_illness AS has_severe_mental_illness WITH SYNONYMS = ('SMI', 'schizophrenia', 'bipolar') COMMENT = 'On SMI register (QOF)',
    conditions.has_anxiety AS has_anxiety COMMENT = 'Anxiety (non-QOF)',
    conditions.has_dementia AS has_dementia COMMENT = 'On dementia register (QOF)',

    -- Neurology Conditions
    conditions.has_epilepsy AS has_epilepsy COMMENT = 'On epilepsy register (QOF, age >=18)',
    conditions.has_parkinsons AS has_parkinsons WITH SYNONYMS = ('PD', 'Parkinsons') COMMENT = 'Parkinsons disease (non-QOF)',
    conditions.has_cerebral_palsy AS has_cerebral_palsy COMMENT = 'Cerebral palsy (non-QOF)',
    conditions.has_mnd AS has_mnd WITH SYNONYMS = ('motor neurone disease', 'ALS') COMMENT = 'Motor neurone disease (non-QOF)',
    conditions.has_multiple_sclerosis AS has_multiple_sclerosis WITH SYNONYMS = ('MS') COMMENT = 'Multiple sclerosis (non-QOF)',

    -- Neurodevelopmental Conditions
    conditions.has_learning_disability AS has_learning_disability WITH SYNONYMS = ('LD', 'learning difficulties') COMMENT = 'On learning disability register (QOF)',
    conditions.has_learning_disability_under_14 AS has_learning_disability_under_14 COMMENT = 'Learning disability under 14 (non-QOF, age 0-13)',
    conditions.has_autism AS has_autism WITH SYNONYMS = ('ASD', 'autism spectrum') COMMENT = 'Autism spectrum disorder (non-QOF)',
    conditions.has_adhd AS has_adhd WITH SYNONYMS = ('ADHD', 'attention deficit') COMMENT = 'ADHD (non-QOF)',

    -- Musculoskeletal Conditions
    conditions.has_rheumatoid_arthritis AS has_rheumatoid_arthritis WITH SYNONYMS = ('RA') COMMENT = 'Rheumatoid arthritis (QOF, age >=16)',
    conditions.has_osteoporosis AS has_osteoporosis COMMENT = 'Osteoporosis (QOF, age >=50)',
    conditions.has_osteoarthritis AS has_osteoarthritis WITH SYNONYMS = ('OA') COMMENT = 'Osteoarthritis (non-QOF)',

    -- Oncology
    conditions.has_cancer AS has_cancer COMMENT = 'On cancer register (QOF)',

    -- Hepatology
    conditions.has_nafld AS has_nafld WITH SYNONYMS = ('NAFLD', 'fatty liver') COMMENT = 'Non-alcoholic fatty liver disease (non-QOF)',
    conditions.has_chronic_liver_disease AS has_chronic_liver_disease WITH SYNONYMS = ('CLD') COMMENT = 'Chronic liver disease (non-QOF)',

    -- Haematology
    conditions.has_sickle_cell AS has_sickle_cell WITH SYNONYMS = ('sickle cell', 'sickle cell disease', 'SCD') COMMENT = 'On sickle cell disease register - an inherited blood disorder causing painful crises and anaemia (non-QOF)',
    conditions.has_thalassaemia AS has_thalassaemia WITH SYNONYMS = ('thalassaemia', 'thalassemia', 'thal') COMMENT = 'On thalassaemia register - an inherited blood disorder causing anaemia, severe forms transfusion-dependent (non-QOF)',

    -- Genetics
    conditions.has_familial_hypercholesterolaemia AS has_familial_hypercholesterolaemia WITH SYNONYMS = ('FH') COMMENT = 'Familial hypercholesterolaemia (non-QOF, age >=20)',

    -- Geriatric
    conditions.has_frailty AS has_frailty COMMENT = 'Recorded frailty (non-QOF)',

    -- Palliative Care
    conditions.has_palliative_care AS has_palliative_care COMMENT = 'On palliative care register (QOF)',

    -- Vulnerability Status
    status.is_care_home_resident AS is_care_home_resident COMMENT = 'Residing in care home',
    status.is_nursing_home_resident AS is_nursing_home_resident COMMENT = 'Residing in nursing home',
    status.care_home_type AS care_home_type COMMENT = 'Type of care home (Residential, Nursing, Dual Registered, Unknown)',
    status.is_homeless_or_chip AS is_homeless_or_chip WITH SYNONYMS = ('homeless', 'rough sleeper', 'NFA') COMMENT = 'Homeless or registered at CHIP practice',
    status.is_housebound AS is_housebound COMMENT = 'Recorded as housebound',
    status.is_carer AS is_carer WITH SYNONYMS = ('unpaid carer', 'caregiver') COMMENT = 'Recorded as unpaid carer',
    status.is_looked_after_child AS is_looked_after_child WITH SYNONYMS = ('LAC', 'looked after', 'in care') COMMENT = 'Looked after child (under 25)',
    status.has_vulnerability_flag AS has_vulnerability_flag COMMENT = 'Has any vulnerability indicator (care home, homeless, housebound, or LAC)',

    -- Smoking & Alcohol
    status.smoking_status AS smoking_status COMMENT = 'Smoking status (Never Smoked, Ex-Smoker, Current Smoker, Unknown)',
    status.alcohol_status AS alcohol_status COMMENT = 'Alcohol status (Non-Drinker, Lower Risk, Increasing Risk, Higher Risk, Dependent, Unknown)',
    status.alcohol_requires_intervention AS alcohol_requires_intervention COMMENT = 'Requires alcohol intervention (AUDIT score >=5)',

    -- Polypharmacy
    status.is_polypharmacy_5plus AS is_polypharmacy_5plus COMMENT = 'Has 5+ current medications',
    status.is_polypharmacy_10plus AS is_polypharmacy_10plus COMMENT = 'Has 10+ current medications (severe polypharmacy)',
    status.medication_count_band AS medication_count_band COMMENT = 'Medication count band (0, 1-4, 5-9, 10-14, 15+)',

    -- Pregnancy
    status.is_currently_pregnant AS is_currently_pregnant COMMENT = 'Currently pregnant',

    -- Data Sharing
    status.is_type1_opted_out AS is_type1_opted_out COMMENT = 'Has Type 1 opt-out',
    status.is_allowed_secondary_use AS is_allowed_secondary_use COMMENT = 'Allowed for secondary use'
)

METRICS(
    -- Population Counts
    demographics.patient_count AS COUNT(DISTINCT demographics.person_id) COMMENT = 'Total number of patients',
    demographics.active_patient_count AS COUNT(DISTINCT CASE WHEN demographics.is_active THEN demographics.person_id END) COMMENT = 'Currently registered patients',
    demographics.deceased_patient_count AS COUNT(DISTINCT CASE WHEN demographics.is_deceased THEN demographics.person_id END) COMMENT = 'Deceased patients',
    demographics.average_age AS AVG(demographics.age) COMMENT = 'Average age of population',

    -- Diabetes Type
    conditions.diabetes_type_1_count AS COUNT(DISTINCT CASE WHEN conditions.diabetes_type = 'Type 1' THEN conditions.person_id END) COMMENT = 'Patients with Type 1 diabetes',
    conditions.diabetes_type_2_count AS COUNT(DISTINCT CASE WHEN conditions.diabetes_type = 'Type 2' THEN conditions.person_id END) COMMENT = 'Patients with Type 2 diabetes',

    -- Cardiovascular Prevalence
    conditions.hypertension_count AS COUNT(DISTINCT CASE WHEN conditions.has_hypertension THEN conditions.person_id END) COMMENT = 'Patients with hypertension',
    conditions.chd_count AS COUNT(DISTINCT CASE WHEN conditions.has_coronary_heart_disease THEN conditions.person_id END) COMMENT = 'Patients with CHD',
    conditions.heart_failure_count AS COUNT(DISTINCT CASE WHEN conditions.has_heart_failure THEN conditions.person_id END) COMMENT = 'Patients with heart failure',
    conditions.af_count AS COUNT(DISTINCT CASE WHEN conditions.has_atrial_fibrillation THEN conditions.person_id END) COMMENT = 'Patients with AF',
    conditions.stroke_tia_count AS COUNT(DISTINCT CASE WHEN conditions.has_stroke_tia THEN conditions.person_id END) COMMENT = 'Patients with stroke/TIA',
    conditions.pad_count AS COUNT(DISTINCT CASE WHEN conditions.has_peripheral_arterial_disease THEN conditions.person_id END) COMMENT = 'Patients with PAD',

    -- Metabolic Prevalence
    conditions.diabetes_count AS COUNT(DISTINCT CASE WHEN conditions.has_diabetes THEN conditions.person_id END) COMMENT = 'Patients with diabetes (all types)',
    conditions.ndh_count AS COUNT(DISTINCT CASE WHEN conditions.has_non_diabetic_hyperglycaemia THEN conditions.person_id END) COMMENT = 'Patients with NDH',
    conditions.ckd_count AS COUNT(DISTINCT CASE WHEN conditions.has_chronic_kidney_disease THEN conditions.person_id END) COMMENT = 'Patients with CKD',
    conditions.obesity_count AS COUNT(DISTINCT CASE WHEN conditions.has_obesity THEN conditions.person_id END) COMMENT = 'Patients with recorded obesity',
    conditions.hypothyroidism_count AS COUNT(DISTINCT CASE WHEN conditions.has_hypothyroidism THEN conditions.person_id END) COMMENT = 'Patients with hypothyroidism',

    -- Respiratory Prevalence
    conditions.copd_count AS COUNT(DISTINCT CASE WHEN conditions.has_copd THEN conditions.person_id END) COMMENT = 'Patients with COPD',
    conditions.asthma_count AS COUNT(DISTINCT CASE WHEN conditions.has_asthma THEN conditions.person_id END) COMMENT = 'Patients with asthma',
    conditions.cyp_asthma_count AS COUNT(DISTINCT CASE WHEN conditions.has_cyp_asthma THEN conditions.person_id END) COMMENT = 'CYP with asthma',

    -- Mental Health Prevalence
    conditions.depression_count AS COUNT(DISTINCT CASE WHEN conditions.has_depression THEN conditions.person_id END) COMMENT = 'Patients with depression',
    conditions.smi_count AS COUNT(DISTINCT CASE WHEN conditions.has_severe_mental_illness THEN conditions.person_id END) COMMENT = 'Patients with SMI',
    conditions.dementia_count AS COUNT(DISTINCT CASE WHEN conditions.has_dementia THEN conditions.person_id END) COMMENT = 'Patients with dementia',
    conditions.anxiety_count AS COUNT(DISTINCT CASE WHEN conditions.has_anxiety THEN conditions.person_id END) COMMENT = 'Patients with anxiety',

    -- Neurology Prevalence
    conditions.epilepsy_count AS COUNT(DISTINCT CASE WHEN conditions.has_epilepsy THEN conditions.person_id END) COMMENT = 'Patients with epilepsy',
    conditions.parkinsons_count AS COUNT(DISTINCT CASE WHEN conditions.has_parkinsons THEN conditions.person_id END) COMMENT = 'Patients with Parkinsons',
    conditions.cerebral_palsy_count AS COUNT(DISTINCT CASE WHEN conditions.has_cerebral_palsy THEN conditions.person_id END) COMMENT = 'Patients with cerebral palsy',
    conditions.mnd_count AS COUNT(DISTINCT CASE WHEN conditions.has_mnd THEN conditions.person_id END) COMMENT = 'Patients with MND',
    conditions.ms_count AS COUNT(DISTINCT CASE WHEN conditions.has_multiple_sclerosis THEN conditions.person_id END) COMMENT = 'Patients with MS',

    -- Neurodevelopmental Prevalence
    conditions.learning_disability_count AS COUNT(DISTINCT CASE WHEN conditions.has_learning_disability THEN conditions.person_id END) COMMENT = 'Patients with LD',
    conditions.autism_count AS COUNT(DISTINCT CASE WHEN conditions.has_autism THEN conditions.person_id END) COMMENT = 'Patients with autism',
    conditions.adhd_count AS COUNT(DISTINCT CASE WHEN conditions.has_adhd THEN conditions.person_id END) COMMENT = 'Patients with ADHD',

    -- Musculoskeletal Prevalence
    conditions.ra_count AS COUNT(DISTINCT CASE WHEN conditions.has_rheumatoid_arthritis THEN conditions.person_id END) COMMENT = 'Patients with RA',
    conditions.osteoporosis_count AS COUNT(DISTINCT CASE WHEN conditions.has_osteoporosis THEN conditions.person_id END) COMMENT = 'Patients with osteoporosis',
    conditions.osteoarthritis_count AS COUNT(DISTINCT CASE WHEN conditions.has_osteoarthritis THEN conditions.person_id END) COMMENT = 'Patients with osteoarthritis',

    -- Other Prevalence
    conditions.cancer_count AS COUNT(DISTINCT CASE WHEN conditions.has_cancer THEN conditions.person_id END) COMMENT = 'Patients with cancer',
    conditions.frailty_count AS COUNT(DISTINCT CASE WHEN conditions.has_frailty THEN conditions.person_id END) COMMENT = 'Patients with frailty',
    conditions.palliative_care_count AS COUNT(DISTINCT CASE WHEN conditions.has_palliative_care THEN conditions.person_id END) COMMENT = 'Patients on palliative care',
    conditions.nafld_count AS COUNT(DISTINCT CASE WHEN conditions.has_nafld THEN conditions.person_id END) COMMENT = 'Patients with NAFLD',
    conditions.cld_count AS COUNT(DISTINCT CASE WHEN conditions.has_chronic_liver_disease THEN conditions.person_id END) COMMENT = 'Patients with chronic liver disease',
    conditions.sickle_cell_count AS COUNT(DISTINCT CASE WHEN conditions.has_sickle_cell THEN conditions.person_id END) COMMENT = 'Patients with sickle cell disease',
    conditions.thalassaemia_count AS COUNT(DISTINCT CASE WHEN conditions.has_thalassaemia THEN conditions.person_id END) COMMENT = 'Patients with thalassaemia',
    conditions.fh_count AS COUNT(DISTINCT CASE WHEN conditions.has_familial_hypercholesterolaemia THEN conditions.person_id END) COMMENT = 'Patients with FH',
    conditions.gestational_diabetes_count AS COUNT(DISTINCT CASE WHEN conditions.has_gestational_diabetes THEN conditions.person_id END) COMMENT = 'Patients with gestational diabetes',

    -- Multimorbidity
    conditions.avg_conditions_per_patient AS AVG(conditions.total_conditions) COMMENT = 'Average conditions per patient',
    conditions.multimorbidity_count AS COUNT(DISTINCT CASE WHEN conditions.total_conditions >= 2 THEN conditions.person_id END) COMMENT = 'Patients with 2+ conditions',
    conditions.complex_multimorbidity_count AS COUNT(DISTINCT CASE WHEN conditions.total_conditions >= 4 THEN conditions.person_id END) COMMENT = 'Patients with 4+ conditions',

    -- Comorbidity Burden (CCMS)
    ccms.avg_ccms AS AVG(ccms.cambridge_comorbidity_score) COMMENT = 'Average Cambridge Comorbidity Score (16+ cohort)',
    ccms.patients_with_ccms AS COUNT(DISTINCT ccms.person_id) COMMENT = 'Patients with a CCMS (aged 16+)',

    -- Smoking
    status.current_smoker_count AS COUNT(DISTINCT CASE WHEN status.smoking_status = 'Current Smoker' THEN status.person_id END) COMMENT = 'Current smokers',
    status.ex_smoker_count AS COUNT(DISTINCT CASE WHEN status.smoking_status = 'Ex-Smoker' THEN status.person_id END) COMMENT = 'Ex-smokers',

    -- Vulnerability
    status.care_home_resident_count AS COUNT(DISTINCT CASE WHEN status.is_care_home_resident THEN status.person_id END) COMMENT = 'Care home residents',
    status.homeless_count AS COUNT(DISTINCT CASE WHEN status.is_homeless_or_chip THEN status.person_id END) COMMENT = 'Homeless patients',
    status.housebound_count AS COUNT(DISTINCT CASE WHEN status.is_housebound THEN status.person_id END) COMMENT = 'Housebound patients',
    status.carer_count AS COUNT(DISTINCT CASE WHEN status.is_carer THEN status.person_id END) COMMENT = 'Unpaid carers',
    status.vulnerable_count AS COUNT(DISTINCT CASE WHEN status.has_vulnerability_flag THEN status.person_id END) COMMENT = 'Patients with any vulnerability',

    -- Polypharmacy
    status.avg_medication_count AS AVG(status.medication_count) COMMENT = 'Average medications per patient',
    status.polypharmacy_5plus_count AS COUNT(DISTINCT CASE WHEN status.is_polypharmacy_5plus THEN status.person_id END) COMMENT = 'Patients with 5+ medications',
    status.polypharmacy_10plus_count AS COUNT(DISTINCT CASE WHEN status.is_polypharmacy_10plus THEN status.person_id END) COMMENT = 'Patients with 10+ medications'
)

COMMENT = 'OLIDS Population Health Semantic View - NCL registered population with demographics, all condition registers (QOF v50), diabetes type, vulnerability factors, and risk behaviours. Source: OLIDS (One London Integrated Data Set — primary care data from system suppliers, unified by the One London team). Grain: one row per person (current state). ESP 2013 weights available via age_band_esp for age-standardised rate calculation.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE, reduce to one row per person before joining on person_id, then aggregate; keep person_id out of the final output. This is one row per person. Example: SELECT borough_resident, AGG(patient_count) FROM SEM_OLIDS_POPULATION WHERE is_active = TRUE GROUP BY borough_resident. Example linkage: reduce active diabetes people here and SGLT2 exposure in sem_olids_prescribing before joining. Use sk_patient_id, not person_id, for sem_sus_acute_activity, sem_cost_index and sem_resource_index. Use borough_registered for practice geography and borough_resident for residence. Prefer IMD 2025. Filter is_active = TRUE unless the user asks about inactive or deceased patients. age_band_esp and esp_proportion are ESP 2013 age-only weights for standardised rates.'
AI_QUESTION_CATEGORIZATION 'Use this view for questions about: condition prevalence (all 40 conditions), diabetes type (T1/T2), demographics, multimorbidity, Cambridge Comorbidity Score (CCMS), vulnerability, smoking, polypharmacy, and population counts. CCMS (cambridge_comorbidity_score) is a continuous weighted score (higher = greater comorbidity burden, can be negative) with NO published risk bands and is NULL for under-16s — report it as a number or average, do not invent thresholds. For clinical biomarkers (BP, HbA1c, BMI, cholesterol) use sem_olids_observations. For serial/over-time biomarker readings use sem_olids_observations_history. For trends over time use sem_olids_trends. Questions needing cohorts from TWO domains (e.g. condition x medication, condition x biomarker control, condition x appointment access) are answerable by joining this view to the other sem_olids_* views on person_id in CTEs, with aggregate-only output.'
