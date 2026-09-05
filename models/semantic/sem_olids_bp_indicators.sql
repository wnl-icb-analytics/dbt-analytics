{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    Grain: one row per person per NICE indicator on the reporting date.

    Covers IND239-246 for hypertension, CHD, stroke/TIA and PAD. A person
    can appear in more than one indicator, so filter indicator_id before
    calculating a rate or comparing populations. Denominators are before
    personalised care adjustments.
#}

TABLES(
    indicators AS {{ ref('fct_person_bp_control_nice_indicators') }}
        PRIMARY KEY (person_id, indicator_id)
        COMMENT = 'NICE IND239-246 BP denominator and numerator status. One row per person per indicator; personalised care adjustments are not applied.',

    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Current demographics, registration, geography and deprivation'
)

RELATIONSHIPS(
    indicators (person_id) REFERENCES demographics
)

FACTS(
    indicators.latest_systolic_bp AS latest_systolic_value COMMENT = 'Latest systolic BP in mmHg',
    indicators.latest_diastolic_bp AS latest_diastolic_value COMMENT = 'Latest diastolic BP in mmHg',
    indicators.indicator_systolic_threshold AS indicator_systolic_threshold COMMENT = 'Exclusive systolic threshold in mmHg specified by the indicator',
    indicators.indicator_diastolic_threshold AS indicator_diastolic_threshold COMMENT = 'Exclusive diastolic threshold in mmHg specified by the indicator',
    demographics.esp_weight AS esp_weight COMMENT = 'ESP 2013 weight for the persons age band out of 100,000',
    demographics.esp_proportion AS esp_proportion COMMENT = 'ESP 2013 weight as a proportion'
)

DIMENSIONS(
    -- Indicator and person grain
    indicators.person_id AS person_id COMMENT = 'Pseudonymised person key for aggregate-only linkage to other sem_olids_* views. Never return person_id in final results.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for aggregate-only linkage to non-OLIDS semantic views. Never return sk_patient_id in final results.',
    indicators.indicator_id AS indicator_id WITH SYNONYMS = ('NICE indicator', 'measure') COMMENT = 'NICE indicator identifier IND239 to IND246. Filter this before using indicator metrics.',
    indicators.indicator_name AS indicator_name COMMENT = 'Published NICE indicator name',
    indicators.condition_name AS condition_name WITH SYNONYMS = ('condition', 'register') COMMENT = 'Register population: Hypertension, Coronary heart disease, Stroke and transient ischaemic attack, or Peripheral arterial disease',
    indicators.reporting_date AS reporting_date WITH SYNONYMS = ('as at date', 'measure date') COMMENT = 'Date on which age and the rolling 12-month window were assessed',
    indicators.age AS age COMMENT = 'Age in years on the reporting date',

    -- Indicator evidence and status
    indicators.latest_bp_date AS latest_bp_date COMMENT = 'Date of latest valid paired BP; null when none is recorded',
    indicators.applied_measurement_context AS applied_measurement_context WITH SYNONYMS = ('BP setting', 'clinic or home') COMMENT = 'CLINIC or HBPM_ABPM context used for the indicator threshold',
    indicators.is_home_bp_event AS is_home_bp_event COMMENT = 'Latest BP was recorded using home monitoring',
    indicators.is_abpm_bp_event AS is_abpm_bp_event COMMENT = 'Latest BP was recorded using ambulatory monitoring',
    indicators.is_in_denominator AS is_in_denominator COMMENT = 'Person is in the indicator denominator before personalised care adjustments; always TRUE on these rows',
    indicators.is_bp_recorded_in_last_12m AS is_bp_recorded_in_last_12m COMMENT = 'Latest BP is within the shared 12-month recommended interval',
    indicators.is_latest_bp_within_indicator_target AS is_latest_bp_within_indicator_target COMMENT = 'Latest BP is below both indicator thresholds, regardless of recording date',
    indicators.is_in_numerator AS is_in_numerator COMMENT = 'Recent BP is below both published indicator thresholds',
    indicators.indicator_status AS indicator_status WITH SYNONYMS = ('achievement status', 'care gap reason') COMMENT = 'ACHIEVED, BP_NOT_RECORDED_IN_LAST_12M, or BP_ABOVE_TARGET',

    -- Demographics
    demographics.gender AS gender COMMENT = 'Patient gender',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age band',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age band',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS standard age band',
    demographics.age_band_esp AS age_band_esp COMMENT = 'ESP 2013 age band',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory',
    demographics.main_language AS main_language COMMENT = 'Main spoken language',
    demographics.interpreter_needed AS interpreter_needed COMMENT = 'Whether an interpreter is required',
    demographics.is_active AS is_active COMMENT = 'Currently registered; filter TRUE for current-population reporting',
    demographics.is_deceased AS is_deceased COMMENT = 'Deceased status',

    -- Organisation and geography
    demographics.registered_practice_code AS practice_code WITH SYNONYMS = ('practice code', 'ODS code', 'GP practice') COMMENT = 'ODS code of the registered GP practice',
    demographics.registered_practice_name AS practice_name COMMENT = 'Registered GP practice name',
    demographics.registered_pcn_code AS pcn_code COMMENT = 'Registered PCN code',
    demographics.registered_pcn_name AS pcn_name WITH SYNONYMS = ('PCN', 'primary care network') COMMENT = 'Registered PCN name',
    demographics.registered_pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'Registered PCN name with borough prefix',
    demographics.borough_registered AS borough_registered COMMENT = 'Registration borough',
    demographics.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB ODS code of the registered practice',
    demographics.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB name of the registered practice',
    demographics.neighbourhood_registered AS neighbourhood_registered COMMENT = 'Registration neighbourhood',
    demographics.borough_resident AS borough_resident COMMENT = 'Residence borough',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Residence neighbourhood',
    demographics.ward_name AS ward_name COMMENT = 'Electoral ward name',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile, where 1 is most deprived',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile'
)

METRICS(
    indicators.denominator_count AS COUNT(DISTINCT CASE WHEN indicators.is_in_denominator THEN indicators.person_id END) COMMENT = 'People in the selected indicator denominator before personalised care adjustments',
    indicators.numerator_count AS COUNT(DISTINCT CASE WHEN indicators.is_in_numerator THEN indicators.person_id END) COMMENT = 'People achieving the selected indicator',
    indicators.care_gap_count AS COUNT(DISTINCT CASE WHEN indicators.is_in_denominator AND NOT indicators.is_in_numerator THEN indicators.person_id END) COMMENT = 'People not achieving the selected indicator before personalised care adjustments',
    indicators.bp_not_recorded_count AS COUNT(DISTINCT CASE WHEN indicators.indicator_status = 'BP_NOT_RECORDED_IN_LAST_12M' THEN indicators.person_id END) COMMENT = 'People without a BP in the preceding 12 months',
    indicators.bp_above_target_count AS COUNT(DISTINCT CASE WHEN indicators.indicator_status = 'BP_ABOVE_TARGET' THEN indicators.person_id END) COMMENT = 'People with a recent BP above the indicator target',
    indicators.achievement_rate AS COUNT(DISTINCT CASE WHEN indicators.is_in_numerator THEN indicators.person_id END) / NULLIF(COUNT(DISTINCT CASE WHEN indicators.is_in_denominator THEN indicators.person_id END), 0) COMMENT = 'Unadjusted indicator achievement rate from 0 to 1'
)

COMMENT = 'OLIDS NICE Blood Pressure Indicators Semantic View - IND239-246 denominator, achievement and care-gap status by condition, age, measurement context and population characteristics. Grain: one row per person per indicator. Personalised care adjustments are not applied.'
AI_SQL_GENERATION 'Filter indicator_id before using metrics because a person can appear in more than one indicator. Use AGG(achievement_rate), or AGG(numerator_count) / AGG(denominator_count), for the selected indicator. Filter is_active = TRUE for current-population reporting. Do not describe these as final QOF performance because personalised care adjustments are not applied. Example: SELECT indicator_id, borough_registered, AGG(denominator_count), AGG(numerator_count), AGG(achievement_rate) FROM SEM_OLIDS_BP_INDICATORS WHERE is_active = TRUE GROUP BY indicator_id, borough_registered. LINKAGE: first filter to one indicator and reduce to one row per person before joining another semantic view on person_id; never return person_id in final results.'
AI_QUESTION_CATEGORIZATION 'Use this view for NICE blood pressure indicators IND239-246, hypertension/CHD/stroke-TIA/PAD BP achievement, missing BP care gaps, BP above-target gaps, and inequalities in these measures by practice, PCN, geography, ethnicity or deprivation. For general patient-specific NG136 BP control or raw latest BP values use sem_olids_observations.'
