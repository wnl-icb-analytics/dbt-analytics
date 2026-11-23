{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    Grain: one row per eligible person, NICE indicator and completed
    month-end over the latest 60 months.

    Monthly companion to sem_olids_nice_indicators over
    fct_person_nice_indicator_status_by_month. Each month is recalculated
    with current rules, code lists and records, so later corrections can
    change past months.
    Demographics, practice and geography join person_month_analysis_base
    on person and month-end. Practice, age and ethnicity are as at that
    month; PCN, borough and neighbourhood use the practice's current
    mapping. Gender, language, residence and deprivation use the current
    record for every month because upstream address history is unavailable.
    Every row is already in the denominator; is_in_denominator is always
    TRUE and is not exposed as a dimension or filter.
    Personalised care adjustments are not applied.
    Secondary-use opt-out filtering is a consumer concern; this view
    does not join opt-out models.
#}

TABLES(
    indicators AS {{ ref('fct_person_nice_indicator_status_by_month') }}
        PRIMARY KEY (person_id, indicator_id, reporting_date)
        COMMENT = 'Monthly NICE indicator results. One row per eligible person, indicator and completed month-end. Personalised care adjustments are not applied.',

    person_months AS {{ ref('person_month_analysis_base') }}
        PRIMARY KEY (person_id, analysis_month)
        COMMENT = 'Demographics, registration, geography and deprivation for each person and completed month'
)

RELATIONSHIPS(
    indicators (person_id, reporting_date) REFERENCES person_months (person_id, analysis_month)
)

DIMENSIONS(
    -- Indicator, person and month grain
    indicators.person_id AS person_id COMMENT = 'Pseudonymised person key for aggregate-only linkage to other sem_olids_* views. Never return person_id in final results.',
    person_months.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for aggregate-only linkage to non-OLIDS semantic views. Never return sk_patient_id in final results.',
    indicators.indicator_id AS indicator_id WITH SYNONYMS = ('NICE indicator', 'measure') COMMENT = 'NICE indicator identifier. Filter or group by this and reporting_date before using metrics because a person can appear in more than one indicator and month.',
    indicators.indicator_name AS indicator_name COMMENT = 'Published NICE indicator name. Join def_indicator for categories and clinical definitions.',
    indicators.clinical_domain AS clinical_domain COMMENT = 'Clinical domain from the current indicator catalogue',
    indicators.clinical_subdomain AS clinical_subdomain COMMENT = 'Condition or clinical topic from the current indicator catalogue',
    indicators.reporting_date AS reporting_date WITH SYNONYMS = ('month', 'month end', 'as at date', 'period') COMMENT = 'Completed month-end on which eligibility and evidence were assessed',
    indicators.measurement_period_start AS measurement_period_start COMMENT = 'Indicator-specific assessment boundary, such as an evidence window or cohort start; its meaning depends on the indicator',
    indicators.age AS age COMMENT = 'Age in years at the month-end, from the approximate birth month',

    -- Time
    person_months.year_number AS year_number COMMENT = 'Calendar year of the month-end',
    person_months.month_number AS month_number COMMENT = 'Calendar month of the month-end (1-12)',
    person_months.month_year_label AS month_year_label COMMENT = 'Readable month and year, such as MAR 2024',
    person_months.financial_year AS financial_year WITH SYNONYMS = ('FY', 'fiscal year') COMMENT = 'UK financial year of the month-end, such as 2023/24',
    person_months.financial_quarter AS financial_quarter WITH SYNONYMS = ('FQ', 'fiscal quarter') COMMENT = 'UK financial quarter of the month-end (Q1 = Apr-Jun)',

    -- Shared status. is_in_denominator is always TRUE on these rows,
    -- so it is not a dimension or filter.
    indicators.is_in_numerator AS is_in_numerator COMMENT = 'Person meets the indicator achievement rule at the month-end',
    indicators.indicator_status AS indicator_status WITH SYNONYMS = ('achievement status', 'care gap reason') COMMENT = 'Accepted tokens only: ACHIEVED, ABOVE_TARGET, NOT_RECORDED_IN_PERIOD, NOT_ASSESSABLE, NOT_TREATED_IN_PERIOD, NEVER_TREATED, VKA_WITHOUT_DOAC_EXCEPTION, DOAC_WHERE_VKA_INDICATED, OUT_OF_RANGE. ACHIEVED when the achievement rule is met; otherwise the reason. Do not invent tokens.',

    -- Demographics. Age bands and ethnicity are as at the month-end;
    -- gender and language use the current record.
    person_months.gender AS gender COMMENT = 'Patient gender; the current value applies to all months',
    person_months.age_band_5y AS age_band_5y COMMENT = '5-year age band at the month-end',
    person_months.age_band_10y AS age_band_10y COMMENT = '10-year age band at the month-end',
    person_months.age_band_nhs AS age_band_nhs COMMENT = 'NHS standard age band at the month-end',
    person_months.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category',
    person_months.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory',
    person_months.main_language AS main_language COMMENT = 'Main spoken language from the current record',
    person_months.interpreter_needed AS interpreter_needed COMMENT = 'Whether an interpreter is required, from the current record',

    -- Organisation. Practice is as at the month-end; PCN, borough, sub-ICB
    -- and neighbourhood use that practice's current mapping.
    indicators.practice_code AS practice_code WITH SYNONYMS = ('practice code', 'ODS code', 'GP practice') COMMENT = 'ODS code of the registration practice attributed at the month-end',
    indicators.practice_name AS practice_name COMMENT = 'Name of the registration practice attributed at the month-end',
    person_months.pcn_code AS pcn_code COMMENT = 'PCN code of the registration practice',
    person_months.pcn_name AS pcn_name WITH SYNONYMS = ('PCN', 'primary care network') COMMENT = 'PCN name of the registration practice',
    person_months.pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'PCN name with borough prefix',
    person_months.borough_registered AS borough_registered COMMENT = 'Borough of the registration practice',
    person_months.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB ODS code of the registration practice',
    person_months.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB name of the registration practice',
    person_months.neighbourhood_registered AS neighbourhood_registered COMMENT = 'Neighbourhood of the registration practice',

    -- Residence and deprivation use the current address for every month;
    -- upstream address history is unavailable.
    person_months.borough_resident AS borough_resident COMMENT = 'Borough of residence from the current address, the same for every month',
    person_months.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Residence neighbourhood from the current address',
    person_months.ward_name AS ward_name COMMENT = 'Electoral ward of the current address',
    person_months.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile of the current address, where 1 is most deprived',
    person_months.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile label of the current address'
)

METRICS(
    indicators.denominator_count AS COUNT(DISTINCT indicators.person_id) COMMENT = 'People on the selected indicator and month rows. Every row is already an eligible denominator person. Only meaningful for one indicator and one reporting_date at a time.',
    indicators.numerator_count AS COUNT(DISTINCT CASE WHEN indicators.is_in_numerator THEN indicators.person_id END) COMMENT = 'People who meet the indicator achievement rule at the month-end',
    indicators.care_gap_count AS COUNT(DISTINCT CASE WHEN NOT indicators.is_in_numerator THEN indicators.person_id END) COMMENT = 'People who do not meet the indicator achievement rule at the month-end. Personalised care adjustments are not applied.',
    indicators.achievement_rate AS COUNT(DISTINCT CASE WHEN indicators.is_in_numerator THEN indicators.person_id END) / NULLIF(COUNT(DISTINCT indicators.person_id), 0) COMMENT = 'Unadjusted rate of people meeting the indicator achievement rule, from 0 to 1. Not final QOF performance.'
)

COMMENT = 'Monthly NICE indicator results over the latest 60 completed month-ends. One row per eligible person, indicator and month-end. Use for trends; use SEM_OLIDS_NICE_INDICATORS for the current position. Each month is recalculated with current rules, code lists and records, so later corrections can change past months. Practice, age and ethnicity are as at each month-end; residence, deprivation, gender and language use the current record. Every row is already in the denominator. Group or filter by indicator_id and reporting_date before rates. Personalised care adjustments are not applied. Secondary-use consumers must apply National Data Opt-Out and Type 1 opt-out filtering themselves. Never return person_id or sk_patient_id.'
AI_SQL_GENERATION 'Always filter or group by indicator_id AND reporting_date before using metrics. A person appears once per indicator per month, so a metric across several months without grouping by reporting_date counts distinct people over the whole span, not a monthly figure. Use AGG(achievement_rate), or AGG(numerator_count) / AGG(denominator_count), per indicator and month. For a financial-year or quarter view, report the last month-end in the period (for example reporting_date = ''2025-03-31'' for 2024/25) rather than pooling months. Every row is already a denominator person; do not filter is_in_denominator. Accepted indicator_status tokens only: ACHIEVED, ABOVE_TARGET, NOT_RECORDED_IN_PERIOD, NOT_ASSESSABLE, NOT_TREATED_IN_PERIOD, NEVER_TREATED, VKA_WITHOUT_DOAC_EXCEPTION, DOAC_WHERE_VKA_INDICATED, OUT_OF_RANGE. Do not invent tokens. Practice is as at each month-end, so a practice trend follows the people registered there each month; PCN, borough and neighbourhood use the current mapping of that practice. Residence, ward and deprivation use the current address of each person for every month, not the address held at the time. Do not describe these rates as final QOF performance because personalised care adjustments are not applied. Secondary-use opt-out filtering is a consumer concern and is not applied in this view. Never return person_id or sk_patient_id in final results. Small-cell suppression is an application concern; do not return person-level rows. Example: SELECT reporting_date, borough_registered, AGG(denominator_count), AGG(numerator_count), AGG(achievement_rate) FROM SEM_OLIDS_NICE_INDICATORS_HISTORY WHERE indicator_id = ''IND278'' GROUP BY reporting_date, borough_registered ORDER BY reporting_date.'
AI_QUESTION_CATEGORIZATION 'Use this view for NICE indicator trends, change over time and month-end comparisons, including inequality trends by practice, PCN, geography, ethnicity or deprivation. For the current position on the build date use sem_olids_nice_indicators, which is fresher than the latest completed month here. Join def_indicator for names, categories and clinical definitions rather than inventing rules. For blood pressure readings and targets use sem_olids_bp_indicators. For diabetes care-process completion use sem_olids_diabetes_care.'
