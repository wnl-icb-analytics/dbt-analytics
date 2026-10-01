{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Population Trends Semantic View
    =====================================

    Time-series semantic model for population health trend analysis.
    OLIDS is the One London Integrated Data Set — primary care data from
    system suppliers (currently EMIS Web, with TPP to follow), unified by
    the One London team.

    Built on person_month_analysis_base with 60-month rolling window.

    Grain: One row per person per month

    Use Cases:
    - Prevalence trends over time
    - Incidence trends (new diagnoses per month)
    - Multimorbidity trends
    - Financial year reporting
    - Age-standardised prevalence trends (via ESP 2013 weights)

    Condition registers built to QOF Business Rules v50.
#}

TABLES(
    trends AS {{ ref('person_month_analysis_base') }}
        PRIMARY KEY (person_id, analysis_month)
        COMMENT = 'Person-month analysis base with 60-month rolling window and condition flags'
)

FACTS(
    trends.total_active_conditions AS total_active_conditions COMMENT = 'Total conditions this month',
    trends.total_new_episodes_this_month AS total_new_episodes_this_month COMMENT = 'New condition episodes this month',
    trends.age AS age COMMENT = 'Age at this analysis month'
)

DIMENSIONS(
    -- Person linkage key
    trends.person_id AS person_id COMMENT = 'Pseudonymised person key, shared by all sem_olids_* views. Exposed only for cross-view cohort intersection: join CTEs over two views on person_id, then aggregate. Never return person_id in final results.',
    trends.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for linkage to non-OLIDS views (SUS acute activity, cost index, resource index). Every active person has one; the underlying person-patient mapping can be many-to-many, so joins remain approximate at the margins. Join CTEs on sk_patient_id after aligning analysis_month, then aggregate; never return sk_patient_id in final results.',

    -- Time Dimensions
    trends.analysis_month AS analysis_month WITH SYNONYMS = ('month', 'date', 'period') COMMENT = 'Month end date for analysis',
    trends.year_number AS year_number COMMENT = 'Calendar year (e.g., 2024)',
    trends.month_number AS month_number COMMENT = 'Calendar month (1-12)',
    trends.quarter_number AS quarter_number COMMENT = 'Calendar quarter (1-4)',
    trends.month_year_label AS month_year_label COMMENT = 'Readable month-year (e.g., MAR 2024)',
    trends.financial_year AS financial_year WITH SYNONYMS = ('FY', 'fiscal year') COMMENT = 'UK financial year (e.g., 2023/24)',
    trends.financial_year_start AS financial_year_start COMMENT = 'Starting year of financial year',
    trends.financial_quarter AS financial_quarter WITH SYNONYMS = ('FQ', 'fiscal quarter') COMMENT = 'UK financial quarter (Q1=Apr-Jun)',
    trends.financial_quarter_number AS financial_quarter_number COMMENT = 'Financial quarter number (1-4)',

    -- Demographics
    trends.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    trends.age_band_5y AS age_band_5y COMMENT = '5-year age bands (0-4, 5-9, ..., 80-84, 85+, Unknown)',
    trends.age_band_10y AS age_band_10y COMMENT = '10-year age bands (0-9, 10-19, ..., 70-79, 80+, Unknown)',
    trends.age_band_nhs AS age_band_nhs COMMENT = 'NHS standard age bands (0-4, 5-14, 15-24, ..., 75-84, 85+)',
    trends.age_band_esp AS age_band_esp COMMENT = 'ESP 2013 age bands (<1, 1-4, 5-9, ..., 80-84, 85-89, 90-94, 95+). ESP weights (esp_proportion) are not in this view — use sem_olids_population for standardised rates.',
    trends.age_life_stage AS age_life_stage COMMENT = 'Life stage (Infant, Toddler, Child, Adolescent, Young Adult, Adult, Older Adult, Elderly, Very Elderly, Unknown)',

    -- Ethnicity
    trends.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    trends.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (White: British, White: Irish, White: Roma, White: Traveller, White: Other White, Mixed: White and Black Caribbean, Mixed: White and Black African, Mixed: White and Asian, Mixed: Other Mixed, Asian: Indian, Asian: Pakistani, Asian: Bangladeshi, Asian: Chinese, Asian: Other Asian, Black: African, Black: Caribbean, Black: Other Black, Other: Arab, Other: Other, Unknown, Not Stated, Not Recorded, Recorded Not Known, Refused)',
    trends.ethnicity_granular AS ethnicity_granular COMMENT = 'Detailed ethnicity classification (Unknown if not recorded)',
    trends.main_language AS main_language COMMENT = 'Main spoken language (Not Recorded if unknown)',
    trends.interpreter_needed AS interpreter_needed COMMENT = 'Whether interpreter is required',

    -- Organisation
    trends.registered_practice_code AS practice_code WITH SYNONYMS = ('practice code', 'ODS code', 'GP practice') COMMENT = 'ODS code of the registered GP practice that month',
    trends.registered_practice_name AS practice_name COMMENT = 'Name of the registered GP practice that month',
    trends.registered_pcn_code AS pcn_code COMMENT = 'PCN code of the registered practice',
    trends.registered_pcn_name AS pcn_name WITH SYNONYMS = ('PCN', 'primary care network') COMMENT = 'PCN name of the registered practice',
    trends.registered_pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'Registered PCN name with borough prefix',
    trends.borough_registered AS borough_registered COMMENT = 'Borough of registration',
    trends.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB / place-based partnership ODS code of the registered practice: 93C = NHS North Central London (Camden, Islington, Barnet, Enfield, Haringey); W2U3Z = NHS North West London (Brent, Ealing, Hammersmith and Fulham, Harrow, Hillingdon, Hounslow, Kensington and Chelsea, Westminster). NULL outside the WNL footprint.',
    trends.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB display name (NHS North Central London or NHS North West London) of the registered practice. NULL outside the WNL footprint.',
    trends.neighbourhood_registered AS neighbourhood_registered COMMENT = 'NCL neighbourhood',

    -- Geography
    trends.lsoa_code_21 AS lsoa_code_21 COMMENT = 'LSOA 2021 code',
    trends.ward_code AS ward_code COMMENT = 'Electoral ward code',
    trends.ward_name AS ward_name COMMENT = 'Electoral ward name',
    trends.borough_resident AS borough_resident COMMENT = 'London borough of residence',
    trends.is_london_resident AS is_london_resident COMMENT = 'Resides in Greater London',
    trends.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Residence neighbourhood',

    -- Deprivation
    trends.imd_decile_19 AS imd_decile_19 COMMENT = 'IMD 2019 decile (1=most deprived, 10=least)',
    trends.imd_quintile_19 AS imd_quintile_19 COMMENT = 'IMD 2019 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',
    trends.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least). Preferred over 2019.',
    trends.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',

    -- Registration Status
    trends.is_active AS is_active COMMENT = 'Active registration this month',
    trends.is_deceased AS is_deceased COMMENT = 'Deceased by this month',

    -- Condition Prevalence Flags (QOF Business Rules v50)
    trends.has_ast AS has_ast WITH SYNONYMS = ('asthma') COMMENT = 'Has asthma this month (QOF)',
    trends.has_copd AS has_copd WITH SYNONYMS = ('COPD') COMMENT = 'Has COPD this month (QOF)',
    trends.has_htn AS has_htn WITH SYNONYMS = ('HTN', 'hypertension', 'high blood pressure') COMMENT = 'Has hypertension this month (QOF)',
    trends.has_chd AS has_chd WITH SYNONYMS = ('CHD', 'coronary heart disease') COMMENT = 'Has CHD this month (QOF)',
    trends.has_af AS has_af WITH SYNONYMS = ('AF', 'atrial fibrillation') COMMENT = 'Has AF this month (QOF)',
    trends.has_hf AS has_hf WITH SYNONYMS = ('heart failure', 'HF') COMMENT = 'Has heart failure this month (QOF)',
    trends.has_pad AS has_pad WITH SYNONYMS = ('PAD', 'peripheral arterial disease') COMMENT = 'Has PAD this month (QOF)',
    trends.has_dm AS has_dm WITH SYNONYMS = ('DM', 'diabetes', 'diabetic') COMMENT = 'Has diabetes this month (QOF)',
    trends.has_ndh AS has_ndh WITH SYNONYMS = ('NDH', 'prediabetes') COMMENT = 'Has NDH this month (QOF)',
    trends.has_dep AS has_dep WITH SYNONYMS = ('depression') COMMENT = 'Has depression this month (QOF)',
    trends.has_smi AS has_smi WITH SYNONYMS = ('SMI', 'severe mental illness') COMMENT = 'Has SMI this month (QOF)',
    trends.has_ckd AS has_ckd WITH SYNONYMS = ('CKD', 'chronic kidney disease') COMMENT = 'Has CKD this month (QOF)',
    trends.has_dem AS has_dem WITH SYNONYMS = ('dementia') COMMENT = 'Has dementia this month (QOF)',
    trends.has_ep AS has_ep WITH SYNONYMS = ('epilepsy') COMMENT = 'Has epilepsy this month (QOF)',
    trends.has_stia AS has_stia WITH SYNONYMS = ('stroke', 'TIA') COMMENT = 'Has stroke/TIA this month (QOF)',
    trends.has_can AS has_can WITH SYNONYMS = ('cancer') COMMENT = 'Has cancer this month (QOF)',
    trends.has_pc AS has_pc WITH SYNONYMS = ('palliative care') COMMENT = 'On palliative care this month (QOF)',
    trends.has_ld AS has_ld WITH SYNONYMS = ('LD', 'learning disability') COMMENT = 'Has LD this month (QOF)',
    trends.has_frail AS has_frail WITH SYNONYMS = ('frailty') COMMENT = 'Has frailty this month (non-QOF)',
    trends.has_ra AS has_ra WITH SYNONYMS = ('RA', 'rheumatoid arthritis') COMMENT = 'Has RA this month (QOF)',
    trends.has_ost AS has_ost WITH SYNONYMS = ('osteoporosis') COMMENT = 'Has osteoporosis this month (QOF)',
    trends.has_nafld AS has_nafld WITH SYNONYMS = ('NAFLD', 'fatty liver') COMMENT = 'Has NAFLD this month (non-QOF)',
    trends.has_fh AS has_fh WITH SYNONYMS = ('FH', 'familial hypercholesterolaemia') COMMENT = 'Has FH this month (non-QOF)',

    -- Incidence Flags (new diagnoses this month)
    trends.new_ast AS new_ast WITH SYNONYMS = ('new asthma') COMMENT = 'New asthma this month',
    trends.new_copd AS new_copd WITH SYNONYMS = ('new COPD') COMMENT = 'New COPD this month',
    trends.new_htn AS new_htn WITH SYNONYMS = ('new hypertension') COMMENT = 'New hypertension this month',
    trends.new_chd AS new_chd WITH SYNONYMS = ('new CHD') COMMENT = 'New CHD this month',
    trends.new_af AS new_af WITH SYNONYMS = ('new AF') COMMENT = 'New AF this month',
    trends.new_hf AS new_hf WITH SYNONYMS = ('new heart failure') COMMENT = 'New heart failure this month',
    trends.new_pad AS new_pad WITH SYNONYMS = ('new PAD') COMMENT = 'New PAD this month',
    trends.new_dm AS new_dm WITH SYNONYMS = ('new diabetes') COMMENT = 'New diabetes this month',
    trends.new_ndh AS new_ndh WITH SYNONYMS = ('new NDH') COMMENT = 'New NDH this month',
    trends.new_gestdiab AS new_gestdiab WITH SYNONYMS = ('gestational diabetes') COMMENT = 'New gestational diabetes this month',
    trends.new_dep AS new_dep WITH SYNONYMS = ('new depression') COMMENT = 'New depression this month',
    trends.new_smi AS new_smi WITH SYNONYMS = ('new SMI') COMMENT = 'New SMI this month',
    trends.new_ckd AS new_ckd WITH SYNONYMS = ('new CKD') COMMENT = 'New CKD this month',
    trends.new_dem AS new_dem WITH SYNONYMS = ('new dementia') COMMENT = 'New dementia this month',
    trends.new_ep AS new_ep WITH SYNONYMS = ('new epilepsy') COMMENT = 'New epilepsy this month',
    trends.new_stia AS new_stia WITH SYNONYMS = ('new stroke', 'new TIA') COMMENT = 'New stroke/TIA this month',
    trends.new_can AS new_can WITH SYNONYMS = ('new cancer') COMMENT = 'New cancer this month',
    trends.new_pc AS new_pc WITH SYNONYMS = ('new palliative care') COMMENT = 'New palliative care this month',
    trends.new_ld AS new_ld WITH SYNONYMS = ('new LD') COMMENT = 'New LD this month',
    trends.new_frail AS new_frail WITH SYNONYMS = ('new frailty') COMMENT = 'New frailty this month',
    trends.new_ra AS new_ra WITH SYNONYMS = ('new RA') COMMENT = 'New RA this month',
    trends.new_ost AS new_ost WITH SYNONYMS = ('new osteoporosis') COMMENT = 'New osteoporosis this month',
    trends.new_nafld AS new_nafld WITH SYNONYMS = ('new NAFLD') COMMENT = 'New NAFLD this month',
    trends.new_fh AS new_fh WITH SYNONYMS = ('new FH') COMMENT = 'New FH this month',

    -- Summary Flags
    trends.has_any_condition AS has_any_condition COMMENT = 'Has any condition this month',
    trends.has_any_new_episode AS has_any_new_episode COMMENT = 'Has any new episode this month'
)

METRICS(
    -- Population
    trends.patient_count AS COUNT(DISTINCT trends.person_id) COMMENT = 'Total patients in period',

    -- Prevalence
    trends.diabetes_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_dm THEN trends.person_id END) COMMENT = 'Patients with diabetes',
    trends.hypertension_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_htn THEN trends.person_id END) COMMENT = 'Patients with hypertension',
    trends.copd_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_copd THEN trends.person_id END) COMMENT = 'Patients with COPD',
    trends.asthma_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ast THEN trends.person_id END) COMMENT = 'Patients with asthma',
    trends.chd_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_chd THEN trends.person_id END) COMMENT = 'Patients with CHD',
    trends.af_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_af THEN trends.person_id END) COMMENT = 'Patients with AF',
    trends.heart_failure_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_hf THEN trends.person_id END) COMMENT = 'Patients with heart failure',
    trends.pad_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_pad THEN trends.person_id END) COMMENT = 'Patients with PAD',
    trends.ckd_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ckd THEN trends.person_id END) COMMENT = 'Patients with CKD',
    trends.ndh_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ndh THEN trends.person_id END) COMMENT = 'Patients with NDH',
    trends.depression_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_dep THEN trends.person_id END) COMMENT = 'Patients with depression',
    trends.smi_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_smi THEN trends.person_id END) COMMENT = 'Patients with SMI',
    trends.dementia_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_dem THEN trends.person_id END) COMMENT = 'Patients with dementia',
    trends.cancer_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_can THEN trends.person_id END) COMMENT = 'Patients with cancer',
    trends.frailty_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_frail THEN trends.person_id END) COMMENT = 'Patients with frailty',
    trends.learning_disability_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ld THEN trends.person_id END) COMMENT = 'Patients with LD',
    trends.epilepsy_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ep THEN trends.person_id END) COMMENT = 'Patients with epilepsy',
    trends.stroke_tia_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_stia THEN trends.person_id END) COMMENT = 'Patients with stroke/TIA',
    trends.palliative_care_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_pc THEN trends.person_id END) COMMENT = 'Patients on palliative care',
    trends.ra_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ra THEN trends.person_id END) COMMENT = 'Patients with RA',
    trends.osteoporosis_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_ost THEN trends.person_id END) COMMENT = 'Patients with osteoporosis',
    trends.nafld_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_nafld THEN trends.person_id END) COMMENT = 'Patients with NAFLD',
    trends.fh_prevalence AS COUNT(DISTINCT CASE WHEN trends.has_fh THEN trends.person_id END) COMMENT = 'Patients with FH',

    -- Incidence
    trends.diabetes_incidence AS COUNT(DISTINCT CASE WHEN trends.new_dm THEN trends.person_id END) COMMENT = 'New diabetes diagnoses',
    trends.hypertension_incidence AS COUNT(DISTINCT CASE WHEN trends.new_htn THEN trends.person_id END) COMMENT = 'New hypertension diagnoses',
    trends.copd_incidence AS COUNT(DISTINCT CASE WHEN trends.new_copd THEN trends.person_id END) COMMENT = 'New COPD diagnoses',
    trends.asthma_incidence AS COUNT(DISTINCT CASE WHEN trends.new_ast THEN trends.person_id END) COMMENT = 'New asthma diagnoses',
    trends.chd_incidence AS COUNT(DISTINCT CASE WHEN trends.new_chd THEN trends.person_id END) COMMENT = 'New CHD diagnoses',
    trends.af_incidence AS COUNT(DISTINCT CASE WHEN trends.new_af THEN trends.person_id END) COMMENT = 'New AF diagnoses',
    trends.heart_failure_incidence AS COUNT(DISTINCT CASE WHEN trends.new_hf THEN trends.person_id END) COMMENT = 'New heart failure diagnoses',
    trends.pad_incidence AS COUNT(DISTINCT CASE WHEN trends.new_pad THEN trends.person_id END) COMMENT = 'New PAD diagnoses',
    trends.ckd_incidence AS COUNT(DISTINCT CASE WHEN trends.new_ckd THEN trends.person_id END) COMMENT = 'New CKD diagnoses',
    trends.depression_incidence AS COUNT(DISTINCT CASE WHEN trends.new_dep THEN trends.person_id END) COMMENT = 'New depression diagnoses',
    trends.smi_incidence AS COUNT(DISTINCT CASE WHEN trends.new_smi THEN trends.person_id END) COMMENT = 'New SMI diagnoses',
    trends.dementia_incidence AS COUNT(DISTINCT CASE WHEN trends.new_dem THEN trends.person_id END) COMMENT = 'New dementia diagnoses',
    trends.epilepsy_incidence AS COUNT(DISTINCT CASE WHEN trends.new_ep THEN trends.person_id END) COMMENT = 'New epilepsy diagnoses',
    trends.cancer_incidence AS COUNT(DISTINCT CASE WHEN trends.new_can THEN trends.person_id END) COMMENT = 'New cancer diagnoses',
    trends.stroke_tia_incidence AS COUNT(DISTINCT CASE WHEN trends.new_stia THEN trends.person_id END) COMMENT = 'New stroke/TIA',
    trends.learning_disability_incidence AS COUNT(DISTINCT CASE WHEN trends.new_ld THEN trends.person_id END) COMMENT = 'New LD diagnoses',
    trends.frailty_incidence AS COUNT(DISTINCT CASE WHEN trends.new_frail THEN trends.person_id END) COMMENT = 'New frailty diagnoses',
    trends.palliative_care_incidence AS COUNT(DISTINCT CASE WHEN trends.new_pc THEN trends.person_id END) COMMENT = 'New palliative care',
    trends.ndh_incidence AS COUNT(DISTINCT CASE WHEN trends.new_ndh THEN trends.person_id END) COMMENT = 'New NDH diagnoses',
    trends.gestational_diabetes_incidence AS COUNT(DISTINCT CASE WHEN trends.new_gestdiab THEN trends.person_id END) COMMENT = 'New gestational diabetes diagnoses',
    trends.ra_incidence AS COUNT(DISTINCT CASE WHEN trends.new_ra THEN trends.person_id END) COMMENT = 'New RA diagnoses',
    trends.osteoporosis_incidence AS COUNT(DISTINCT CASE WHEN trends.new_ost THEN trends.person_id END) COMMENT = 'New osteoporosis diagnoses',
    trends.nafld_incidence AS COUNT(DISTINCT CASE WHEN trends.new_nafld THEN trends.person_id END) COMMENT = 'New NAFLD diagnoses',
    trends.fh_incidence AS COUNT(DISTINCT CASE WHEN trends.new_fh THEN trends.person_id END) COMMENT = 'New FH diagnoses',

    -- Multimorbidity
    trends.avg_conditions_per_patient AS AVG(trends.total_active_conditions) COMMENT = 'Average conditions per patient',
    trends.multimorbidity_count AS COUNT(DISTINCT CASE WHEN trends.total_active_conditions >= 2 THEN trends.person_id END) COMMENT = 'Patients with 2+ conditions',
    trends.complex_multimorbidity_count AS COUNT(DISTINCT CASE WHEN trends.total_active_conditions >= 4 THEN trends.person_id END) COMMENT = 'Patients with 4+ conditions',

    -- Total Incidence
    trends.sum_new_episodes AS SUM(trends.total_new_episodes_this_month) COMMENT = 'Total new episodes',
    trends.patients_with_new_episode AS COUNT(DISTINCT CASE WHEN trends.has_any_new_episode THEN trends.person_id END) COMMENT = 'Patients with any new episode',

    -- Demographics
    trends.average_age AS AVG(trends.age) COMMENT = 'Average age'
)

COMMENT = 'OLIDS Population Trends Semantic View - 60-month time series for condition prevalence, incidence, and multimorbidity trends. Source: OLIDS (One London Integrated Data Set). Condition registers built to QOF Business Rules v50. Grain: one row per person per month. age_band_esp available for grouping but ESP weights are not in this view — use sem_olids_population for age-standardised rates.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE, reduce to one row per person before joining on person_id, then aggregate; keep person_id out of the final output. This is person-month grain: align analysis_month before linkage. analysis_month is a month-END date; sem_cost_index.activity_month is a month-START date — align on DATE_TRUNC(month, ...), never raw date equality. Example: SELECT financial_year, AGG(patient_count), AGG(diabetes_prevalence) FROM SEM_OLIDS_TRENDS WHERE is_active = TRUE GROUP BY financial_year. Example linkage: reduce an aligned monthly cohort here and events in sem_olids_appointments before joining. Use financial_year or financial_quarter for UK reporting. is_active = TRUE is for active-registration cohorts; retain inactive historical non-deceased rows when asked. age_band_esp is available for grouping; ESP 2013 age-only weights are in sem_olids_population.'
AI_QUESTION_CATEGORIZATION 'Use this view for questions about: trends over time, prevalence changes, incidence rates, year-on-year comparisons, financial year reporting, and monthly/quarterly analysis. For current state snapshots use sem_olids_population. For clinical biomarkers use sem_olids_observations. Questions needing cohorts from TWO domains (e.g. incidence x medication initiation) are answerable by joining this view to the other sem_olids_* views on person_id in CTEs, with aggregate-only output.'
