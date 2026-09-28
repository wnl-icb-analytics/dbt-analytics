{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Screening Semantic View
    =============================

    National screening programme status (bowel, breast, cervical) plus
    NHS Health Check eligibility, per person. OLIDS is the One London
    Integrated Data Set — primary care data from system suppliers,
    unified by the One London team.

    Grain: one row per person per programme table. Each programme table
    is pre-filtered to its eligible cohort:
    - bowel:    age 50-74, all sexes, 2.5-year interval (QOF v50)
    - breast:   female, age 50-70, 3-year interval
    - cervical: female, age 25-64, 3.5-year (25-49) / 5.5-year (50-64)
                intervals (QOF v50)
    - health check: whole population with an eligibility flag
                (age 40-74, none of 7 excluding CVD-risk registers)

    Programme tables include inactive and deceased persons — filter
    is_active = TRUE for coverage reporting.
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics, geography, ethnicity, deprivation (current snapshot)',

    bowel AS {{ ref('fct_bowel_screening_status') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Bowel screening status, age 50-74 cohort, 2.5-year interval (QOF v50)',

    breast AS {{ ref('fct_breast_screening_status') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Breast screening status, female 50-70 cohort, 3-year interval',

    cervical AS {{ ref('fct_cervical_screening_status') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Cervical screening status, female 25-64 cohort, age-dependent interval (3.5y / 5.5y, QOF v50)',

    hc AS {{ ref('dim_nhs_health_check_eligibility') }}
        PRIMARY KEY (person_id)
        COMMENT = 'NHS Health Check eligibility (age 40-74, no excluding CVD-risk condition) and due status (5-year rule)'
)

RELATIONSHIPS(
    bowel (person_id) REFERENCES demographics,
    breast (person_id) REFERENCES demographics,
    cervical (person_id) REFERENCES demographics,
    hc (person_id) REFERENCES demographics
)

FACTS(
    bowel.bowel_screening_interval_years AS screening_interval_years COMMENT = 'Bowel screening interval in years for the eligible person',
    bowel.bowel_days_overdue AS days_overdue COMMENT = 'Days past the bowel screening due date; null when up to date or never screened',
    breast.breast_screening_interval_years AS screening_interval_years COMMENT = 'Breast screening interval in years for the eligible person',
    breast.breast_days_overdue AS days_overdue COMMENT = 'Days past the breast screening due date; null when up to date or never screened',
    cervical.cervical_screening_interval_years AS screening_interval_years COMMENT = 'Cervical screening interval in years for the eligible person; depends on age',
    cervical.cervical_days_overdue AS days_overdue COMMENT = 'Days past the cervical screening due date; null when up to date, unsuitable or never screened'
)

DIMENSIONS(
    -- Person linkage key
    demographics.person_id AS person_id COMMENT = 'Pseudonymised person key, shared by all sem_olids_* views. Exposed only for cross-view cohort intersection: join CTEs over two views on person_id, then aggregate. Never return person_id in final results.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for linkage to non-OLIDS views (SUS acute activity, cost index, resource index). Every active person has one; the underlying person-patient mapping can be many-to-many, so joins remain approximate at the margins. Join CTEs on sk_patient_id, then aggregate; never return sk_patient_id in final results.',

    -- Bowel screening (age 50-74)
    bowel.bowel_screening_status AS programme_status WITH SYNONYMS = ('bowel status') COMMENT = 'Bowel programme status (Never Screened, Up to Date, Overdue, Unknown)',
    bowel.bowel_latest_screening_type AS latest_screening_type COMMENT = 'Type of the latest recorded bowel screening event',
    bowel.bowel_latest_screening_date AS latest_screening_date COMMENT = 'Date of the latest recorded bowel screening event, including completed and declined events',
    bowel.bowel_latest_completed_date AS latest_completed_date COMMENT = 'Date of most recent completed bowel screening',
    bowel.bowel_next_due_date AS next_screening_due_date COMMENT = 'Bowel due date (latest completed + 2.5 years), not an activity date. Future means not yet due; past means overdue.',
    bowel.bowel_never_screened AS never_screened COMMENT = 'No completed bowel screening ever',
    bowel.bowel_latest_is_declined AS latest_is_declined COMMENT = 'Latest bowel record is a decline',

    -- Breast screening (female 50-70)
    breast.breast_screening_status AS programme_status COMMENT = 'Breast programme status (Never Screened, Up to Date, Overdue, Unknown)',
    breast.breast_latest_screening_type AS latest_screening_type COMMENT = 'Type of the latest recorded breast screening event',
    breast.breast_latest_screening_date AS latest_screening_date COMMENT = 'Date of the latest recorded breast screening event, including completed and declined events',
    breast.breast_latest_completed_date AS latest_completed_date COMMENT = 'Date of most recent completed breast screening',
    breast.breast_next_due_date AS next_screening_due_date COMMENT = 'Breast due date (latest completed + 3 years), not an activity date. Future means not yet due; past means overdue.',
    breast.breast_never_screened AS never_screened COMMENT = 'No completed breast screening ever',
    breast.breast_latest_is_declined AS latest_is_declined COMMENT = 'Latest breast record is a decline',

    -- Cervical screening (female 25-64)
    cervical.cervical_screening_status AS programme_status COMMENT = 'Cervical programme status (Unsuitable, Never Screened, Up to Date, Overdue, Unknown)',
    cervical.cervical_latest_screening_type AS latest_screening_type COMMENT = 'Type of the latest recorded cervical screening event',
    cervical.cervical_latest_screening_date AS latest_screening_date COMMENT = 'Date of the latest recorded cervical screening event, including completed, declined, unsuitable and non-response events',
    cervical.cervical_latest_completed_date AS latest_completed_date COMMENT = 'Date of most recent completed cervical screening',
    cervical.cervical_next_due_date AS next_screening_due_date COMMENT = 'Cervical due date (3.5 years at 25-49, 5.5 years at 50-64), not an activity date. Future means not yet due; past means overdue.',
    cervical.cervical_never_screened AS never_screened COMMENT = 'No completed cervical screening ever',
    cervical.cervical_latest_is_declined AS latest_is_declined COMMENT = 'Latest cervical record is a decline',
    cervical.cervical_latest_is_unsuitable AS latest_is_unsuitable COMMENT = 'Latest cervical record marks person unsuitable',
    cervical.cervical_latest_is_non_response AS latest_is_non_response COMMENT = 'Latest cervical record is a non-response to invitation',

    -- NHS Health Check (whole population, eligibility as flag)
    hc.is_eligible_for_nhs_health_check AS is_eligible_for_nhs_health_check WITH SYNONYMS = ('health check eligible') COMMENT = 'Age 40-74 and not on any of the 7 excluding CVD-risk registers (CHD, diabetes, stroke/TIA, CKD, AF, HF, FH)',
    hc.due_nhs_health_check AS due_nhs_health_check WITH SYNONYMS = ('health check due') COMMENT = 'Eligible AND never checked or last check over 5 years ago',
    hc.latest_health_check_date AS latest_health_check_date COMMENT = 'Date of latest NHS Health Check',
    hc.has_any_excluding_condition AS has_any_excluding_condition COMMENT = 'On any register that excludes from NHS Health Check',

    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS Digital standard age bands',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (detailed groupings)',
    demographics.main_language AS main_language COMMENT = 'Main spoken language (Not Recorded if unknown)',
    demographics.interpreter_needed AS interpreter_needed COMMENT = 'Whether interpreter is required',
    demographics.is_active AS is_active COMMENT = 'Currently registered. Programme tables include inactive/deceased — filter TRUE for coverage.',
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
    -- Bowel
    bowel.bowel_eligible_count AS COUNT(DISTINCT bowel.person_id) COMMENT = 'People in the bowel screening cohort (50-74)',
    bowel.bowel_up_to_date_count AS COUNT(DISTINCT CASE WHEN bowel.programme_status = 'Up to Date' THEN bowel.person_id END) COMMENT = 'Bowel screening up to date',
    bowel.bowel_overdue_count AS COUNT(DISTINCT CASE WHEN bowel.programme_status = 'Overdue' THEN bowel.person_id END) COMMENT = 'Bowel screening overdue',
    bowel.bowel_never_screened_count AS COUNT(DISTINCT CASE WHEN bowel.never_screened THEN bowel.person_id END) COMMENT = 'Never bowel screened',

    -- Breast
    breast.breast_eligible_count AS COUNT(DISTINCT breast.person_id) COMMENT = 'People in the breast screening cohort (female 50-70)',
    breast.breast_up_to_date_count AS COUNT(DISTINCT CASE WHEN breast.programme_status = 'Up to Date' THEN breast.person_id END) COMMENT = 'Breast screening up to date',
    breast.breast_overdue_count AS COUNT(DISTINCT CASE WHEN breast.programme_status = 'Overdue' THEN breast.person_id END) COMMENT = 'Breast screening overdue',
    breast.breast_never_screened_count AS COUNT(DISTINCT CASE WHEN breast.never_screened THEN breast.person_id END) COMMENT = 'Never breast screened',

    -- Cervical
    cervical.cervical_eligible_count AS COUNT(DISTINCT cervical.person_id) COMMENT = 'People in the cervical screening cohort (female 25-64)',
    cervical.cervical_up_to_date_count AS COUNT(DISTINCT CASE WHEN cervical.programme_status = 'Up to Date' THEN cervical.person_id END) COMMENT = 'Cervical screening up to date',
    cervical.cervical_overdue_count AS COUNT(DISTINCT CASE WHEN cervical.programme_status = 'Overdue' THEN cervical.person_id END) COMMENT = 'Cervical screening overdue',
    cervical.cervical_never_screened_count AS COUNT(DISTINCT CASE WHEN cervical.never_screened THEN cervical.person_id END) COMMENT = 'Never cervically screened',
    cervical.cervical_unsuitable_count AS COUNT(DISTINCT CASE WHEN cervical.programme_status = 'Unsuitable' THEN cervical.person_id END) COMMENT = 'Marked unsuitable for cervical screening',

    -- NHS Health Check
    hc.hc_population_count AS COUNT(DISTINCT hc.person_id) COMMENT = 'People in the NHS Health Check model (whole population)',
    hc.hc_eligible_count AS COUNT(DISTINCT CASE WHEN hc.is_eligible_for_nhs_health_check THEN hc.person_id END) COMMENT = 'People eligible for an NHS Health Check',
    hc.hc_due_count AS COUNT(DISTINCT CASE WHEN hc.due_nhs_health_check THEN hc.person_id END) COMMENT = 'People due an NHS Health Check'
)

COMMENT = 'OLIDS Screening Semantic View - bowel, breast, and cervical screening status plus NHS Health Check eligibility/due status. Grain: one row per person per programme table; each screening table is pre-filtered to its age/sex-eligible cohort. Programme tables include inactive/deceased persons — filter is_active = TRUE for coverage reporting.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE, reduce to one row per person before joining on person_id, then aggregate; keep person_id out of the final output. Coverage is AGG(<programme>_up_to_date_count) / AGG(<programme>_eligible_count) with is_active = TRUE. The <programme>_next_due_date dimensions are due dates, not event dates: future = not yet due, past = overdue. Never use them in activity windows. <programme>_days_overdue is null for people who are up to date or have never completed screening; filter <programme>_screening_status = ''Overdue'' before summarising it. <programme>_latest_screening_date includes any recorded outcome, while <programme>_latest_completed_date includes completed screening only. Example: SELECT borough_resident, AGG(bowel_eligible_count), AGG(bowel_up_to_date_count) FROM SEM_OLIDS_SCREENING WHERE is_active = TRUE GROUP BY borough_resident. Example linkage: reduce overdue people here and SMI people in sem_olids_population before joining. Query one programme table at a time; combining programme tables multiplies rows. Each programme table is already restricted to its eligible cohort (bowel 50-74 all sexes; breast female 50-70; cervical female 25-64) — do not add age/sex filters unless narrowing further. Up-to-date intervals per QOF v50: bowel 2.5y, breast 3y, cervical 3.5y (25-49) / 5.5y (50-64). NHS Health Check eligibility excludes people on CVD-risk registers (has_any_excluding_condition); due means eligible and more than five years since the last check.'
AI_QUESTION_CATEGORIZATION 'Use this view for: bowel/breast/cervical screening coverage, overdue and never-screened cohorts, screening declines, NHS Health Check eligibility and due status, and screening equity by deprivation/ethnicity/language/practice. For condition registers use sem_olids_population. For diabetic retinal screening use sem_olids_diabetes_care.'
