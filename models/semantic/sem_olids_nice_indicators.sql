{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    Grain: one row per eligible person per NICE indicator, assessed on
    the build date.

    Cross-indicator status view over fct_person_nice_indicator_status.
    Each indicator keeps its own population, time window and clinical
    criteria. A person can appear in more than one indicator, so filter
    or group by indicator_id before calculating a rate.
    Every row is already in the denominator; is_in_denominator is always
    TRUE and is not exposed as a dimension or filter.
    Personalised care adjustments are not applied.
    Secondary-use opt-out filtering is a consumer concern; this view
    does not join opt-out models.
#}

TABLES(
    indicators AS {{ ref('fct_person_nice_indicator_status') }}
        PRIMARY KEY (person_id, indicator_id)
        COMMENT = 'Current NICE indicator results. One row per eligible person and indicator, assessed on the build date. Personalised care adjustments are not applied.',

    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Current demographics, registration, geography and deprivation'
)

RELATIONSHIPS(
    indicators (person_id) REFERENCES demographics
)

FACTS(
    demographics.esp_weight AS esp_weight COMMENT = 'ESP 2013 weight for the persons age band out of 100,000',
    demographics.esp_proportion AS esp_proportion COMMENT = 'ESP 2013 weight as a proportion'
)

DIMENSIONS(
    -- Indicator and person grain
    indicators.person_id AS person_id COMMENT = 'Pseudonymised person key for aggregate-only linkage to other sem_olids_* views. Never return person_id in final results.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for aggregate-only linkage to non-OLIDS semantic views. Never return sk_patient_id in final results.',
    indicators.indicator_id AS indicator_id WITH SYNONYMS = ('NICE indicator', 'measure') COMMENT = 'NICE indicator identifier. Filter or group by this before using metrics because a person can appear in more than one indicator, and each indicator has its own population and time window.',
    indicators.indicator_name AS indicator_name COMMENT = 'Published NICE indicator name. Join def_indicator for categories and clinical definitions.',
    indicators.reporting_date AS reporting_date WITH SYNONYMS = ('build date', 'as at date', 'measure date') COMMENT = 'Build date on which eligibility and the measurement period were assessed',
    indicators.measurement_period_start AS measurement_period_start COMMENT = 'Inclusive start of the indicator measurement period; the length depends on the indicator',
    indicators.age AS age COMMENT = 'Age in years on the reporting date; null when no birth date is recorded',

    -- Shared status. is_in_denominator is always TRUE on these rows,
    -- so it is not a dimension or filter.
    indicators.is_in_numerator AS is_in_numerator COMMENT = 'Person meets the selected indicator achievement rule on the build date',
    indicators.indicator_status AS indicator_status WITH SYNONYMS = ('achievement status', 'care gap reason') COMMENT = 'Accepted tokens only: ACHIEVED, ABOVE_TARGET, NOT_RECORDED_IN_PERIOD, NOT_ASSESSABLE, NOT_TREATED_IN_PERIOD, NEVER_TREATED, VKA_WITHOUT_DOAC_EXCEPTION, DOAC_WHERE_VKA_INDICATED, OUT_OF_RANGE. ACHIEVED when the achievement rule is met; otherwise the reason. An invalid latest result in the measurement window is NOT_ASSESSABLE and is not replaced by an older result. OUT_OF_RANGE means the latest result in the period is outside the target range. Do not invent tokens.',

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
    demographics.is_active AS is_active COMMENT = 'Currently registered. Upstream status rows are already currently registered, living and non-test, so filtering TRUE is belt-and-braces, not a second eligibility rule.',
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
    indicators.denominator_count AS COUNT(DISTINCT indicators.person_id) COMMENT = 'People on the selected indicator rows. Every row is already an eligible denominator person; this does not filter is_in_denominator.',
    indicators.numerator_count AS COUNT(DISTINCT CASE WHEN indicators.is_in_numerator THEN indicators.person_id END) COMMENT = 'People who meet the selected indicator achievement rule on the build date',
    indicators.care_gap_count AS COUNT(DISTINCT CASE WHEN NOT indicators.is_in_numerator THEN indicators.person_id END) COMMENT = 'People who do not meet the selected indicator achievement rule on the build date. Personalised care adjustments are not applied.',
    indicators.achievement_rate AS COUNT(DISTINCT CASE WHEN indicators.is_in_numerator THEN indicators.person_id END) / NULLIF(COUNT(DISTINCT indicators.person_id), 0) COMMENT = 'Unadjusted rate of people meeting the selected indicator achievement rule, from 0 to 1. Not final QOF performance.'
)

COMMENT = 'Current NICE indicator results. One row per eligible person and indicator, assessed on the build date. People must be currently registered, living and non-test, and meet the individual indicator population rules. Every row is already in the denominator; do not filter is_in_denominator. Filter or group by indicator_id before rates because a person can appear in more than one indicator. indicator_status is the reason when the achievement rule is not met. Family measures hold readings, thresholds and evidence. Personalised care adjustments are not applied. Secondary-use consumers must apply National Data Opt-Out and Type 1 opt-out filtering themselves. Never return person_id or sk_patient_id.'
AI_SQL_GENERATION 'Filter indicator_id, or group by indicator_id, before using metrics because a person can appear in more than one indicator and each indicator has its own population and time window. Every row is already a denominator person; denominator_count is COUNT DISTINCT person_id after that selection, not a filter on is_in_denominator. Use AGG(achievement_rate), or AGG(numerator_count) / AGG(denominator_count), for the selected indicator. Group by indicator_status or is_in_numerator for achievement and unachieved-reason breakdowns. Accepted indicator_status tokens only: ACHIEVED, ABOVE_TARGET, NOT_RECORDED_IN_PERIOD, NOT_ASSESSABLE, NOT_TREATED_IN_PERIOD, NEVER_TREATED, VKA_WITHOUT_DOAC_EXCEPTION, DOAC_WHERE_VKA_INDICATED, OUT_OF_RANGE. Do not invent tokens. An invalid latest result in the measurement window is NOT_ASSESSABLE and is not replaced by an older result; how a valid latest result is selected stays on the family measure. The upstream status population is already currently registered, living and non-test, so filtering is_active = TRUE is belt-and-braces, not a second eligibility rule. Do not describe these as final QOF performance because personalised care adjustments are not applied. Secondary-use opt-out filtering is a consumer concern and is not applied in this view. Never return person_id or sk_patient_id in final results. Small-cell suppression is an application concern; do not return person-level rows. Example: SELECT indicator_id, borough_registered, AGG(denominator_count), AGG(numerator_count), AGG(achievement_rate) FROM SEM_OLIDS_NICE_INDICATORS WHERE is_active = TRUE GROUP BY indicator_id, borough_registered. LINKAGE: first filter to one indicator and reduce to one row per person before joining another semantic view on person_id; never return person_id in final results.'
AI_QUESTION_CATEGORIZATION 'Use this view for current NICE indicator achievement, unachieved reasons and inequalities by practice, PCN, geography, ethnicity or deprivation across the people-level results in fct_person_nice_indicator_status. Each indicator keeps its own population, time window and clinical criteria; join def_indicator for names, categories and clinical definitions rather than inventing rules here. For IND239 to IND246 blood pressure readings, targets and measurement context use sem_olids_bp_indicators. For diabetes care-process completion and triple targets use sem_olids_diabetes_care; those care-process counts are outside this person-level status view. For general latest biomarker values use sem_olids_observations.'
