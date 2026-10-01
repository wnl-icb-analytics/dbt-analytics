{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Clinical Observations History Semantic View
    =================================================

    Serial / over-time companion to sem_olids_observations. Where that view
    holds the LATEST value per biomarker (one row per person), this view holds
    EVERY recorded reading (one row per observation event) so you can answer
    "latest 2", trajectory, variability, and recheck-interval questions.

    OLIDS is the One London Integrated Data Set — primary care data from system
    suppliers (currently EMIS Web, with TPP to follow), unified by the One
    London team.

    Grain: One row per observation event (person x biomarker x date).

    Retention asymmetry (important):
    OLIDS keeps FULL history for currently-registered persons, but only ~5
    years (60 months) for persons who have left or died. Unlike
    sem_olids_appointments and sem_olids_trends, this view is deliberately NOT
    capped at 60 months — that is the point of it, so long-run trajectories for
    current registrants (e.g. a 55-year-old's BMI over 10 years) are usable.
    Consequence: readings older than ~5 years exist mainly for people still
    registered now, so long-run POPULATION cross-sections (e.g. average BMI by
    calendar year over a decade) are survivor-biased. Use this view for
    per-person trajectories and change within the currently-registered cohort
    (filter is_active = TRUE); do not read pre-5-year population averages as
    representative of the population at that time.

    Design:
    - Long format: filter to ONE observation_type before reading `value`.
      Values are only comparable within a single biomarker.
    - For "latest N", window with ROW_NUMBER() OVER (PARTITION BY person_id,
      observation_type ORDER BY clinical_effective_date DESC,
      observation_event_id DESC) — the event id breaks ties when a person has
      multiple readings of one biomarker on the same date.
    - Blood pressure is split into 'Systolic BP' and 'Diastolic BP' rows.

    Biomarkers: Systolic BP, Diastolic BP, Total Cholesterol, LDL Cholesterol,
    QRISK, HbA1c, BMI, Waist Circumference, eGFR, Creatinine, Urine ACR, ALT,
    GGT, Bilirubin, Haemoglobin, Platelets, Eosinophils, GP-recorded Electronic
    Frailty Index (eFI/eFI2), Rockwood Frailty Scale. Calculated eFI2 has no
    serial history and is only in sem_olids_observations.
#}

TABLES(
    obs AS {{ ref('int_observation_events_long') }}
        PRIMARY KEY (observation_event_id)
        COMMENT = 'One row per biomarker observation event. Filter to a single observation_type before aggregating value.',

    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics, geography, ethnicity, deprivation (current snapshot)'
)

RELATIONSHIPS(
    obs (person_id) REFERENCES demographics
)

FACTS(
    obs.value AS value COMMENT = 'Numeric result. Only comparable within a single observation_type (see unit).'
)

DIMENSIONS(
    -- Person linkage key (on obs so it can be selected alongside the value fact)
    obs.person_id AS person_id COMMENT = 'Pseudonymised person key, shared by all sem_olids_* views. Use for latest-N windowing within this view and for cross-view cohort intersection (join CTEs over two views on person_id, then aggregate). Never return person_id in final results.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for linkage to non-OLIDS views (SUS acute activity, cost index, resource index). Every active person has one; the underlying person-patient mapping can be many-to-many, so joins remain approximate at the margins. Join CTEs on sk_patient_id, then aggregate; never return sk_patient_id in final results.',

    -- Observation
    obs.observation_event_id AS observation_event_id COMMENT = 'Stable per-event surrogate key. Use as the final ORDER BY tiebreaker in latest-N windows so tied clinical_effective_dates rank deterministically.',
    obs.observation_type AS observation_type WITH SYNONYMS = ('biomarker', 'measurement', 'test') COMMENT = 'Biomarker label. Filter to one value before aggregating: Systolic BP, Diastolic BP, Total Cholesterol, LDL Cholesterol, QRISK, HbA1c, BMI, Waist Circumference, eGFR, Creatinine, Urine ACR, ALT, GGT, Bilirubin, Haemoglobin, Platelets, Eosinophils, GP-recorded Electronic Frailty Index (eFI/eFI2), Rockwood Frailty Scale. Calculated eFI2 is not in this history view.',
    obs.observation_group AS observation_group COMMENT = 'Clinical group (Cardiovascular, Metabolic, Renal, Liver, Haematology, Frailty)',
    obs.clinical_effective_date AS clinical_effective_date WITH SYNONYMS = ('observation date', 'test date', 'date') COMMENT = 'Date of the reading',
    obs.unit AS unit COMMENT = 'Unit of measure for value',
    obs.category AS category COMMENT = 'Type-specific clinical category; filter observation_type first. All observation types except Diastolic BP are categorised. Systolic BP is Hypertensive range or Below hypertensive range; HbA1c, BMI, eGFR, cholesterol, renal, liver, haematology, QRISK, waist and frailty types use their upstream clinical categories.',
    obs.hypertension_stage AS hypertension_stage COMMENT = 'NICE NG136 stage for paired BP events: Normal, Stage 1, Stage 2, or Stage 3 (Severe). Uses the event''s clinic or home/ABPM context; NULL for non-BP types.',

    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age AS age COMMENT = 'Current age in years (drifts — reading dates are event-time)',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands (0-4, 5-9, ..., 80-84, 85+, Unknown)',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands (0-9, 10-19, ..., 70-79, 80+, Unknown)',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS Digital standard age bands (0-4, 5-14, 15-24, ..., 75-84, 85+)',
    demographics.age_band_esp AS age_band_esp COMMENT = 'ESP 2013 age bands (<1, 1-4, 5-9, ..., 80-84, 85-89, 90-94, 95+)',
    demographics.age_life_stage AS age_life_stage COMMENT = 'Life stage (Infant, Toddler, Child, Adolescent, Young Adult, Adult, Older Adult, Elderly, Very Elderly, Unknown)',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (detailed groupings; Unknown/Not Stated/Not Recorded/Refused where missing)',
    demographics.ethnicity_granular AS ethnicity_granular COMMENT = 'Detailed ethnicity classification (Unknown if not recorded)',
    demographics.main_language AS main_language COMMENT = 'Main spoken language (Not Recorded if unknown)',
    demographics.interpreter_needed AS interpreter_needed COMMENT = 'Whether interpreter is required',
    demographics.is_active AS is_active COMMENT = 'Currently registered with NCL GP practice',
    demographics.is_deceased AS is_deceased COMMENT = 'Deceased status',

    -- Organisation
    demographics.registered_practice_code AS practice_code WITH SYNONYMS = ('practice code', 'ODS code', 'GP practice') COMMENT = 'ODS code of the patient''s registered GP practice',
    demographics.registered_practice_name AS practice_name COMMENT = 'Name of the patient''s registered GP practice',
    demographics.registered_pcn_code AS pcn_code COMMENT = 'PCN code of the registered practice',
    demographics.registered_pcn_name AS pcn_name WITH SYNONYMS = ('PCN', 'primary care network') COMMENT = 'PCN name of the registered practice',
    demographics.registered_pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'Registered PCN name with borough prefix',
    demographics.borough_registered AS borough_registered COMMENT = 'Registration borough',
    demographics.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB / place-based partnership ODS code of the registered practice: 93C = NHS North Central London; W2U3Z = NHS North West London. NULL outside the WNL footprint.',
    demographics.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB display name (NHS North Central London or NHS North West London). NULL outside the WNL footprint.',
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
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)'
)

METRICS(
    obs.observation_count AS COUNT(obs.observation_event_id) COMMENT = 'Number of observation events (filter to one observation_type)',
    obs.patient_count AS COUNT(DISTINCT obs.person_id) COMMENT = 'Distinct patients with a reading (filter to one observation_type)',
    obs.avg_value AS AVG(obs.value) COMMENT = 'Average value — only meaningful when filtered to a single observation_type',
    obs.min_value AS MIN(obs.value) COMMENT = 'Minimum value (filter to one observation_type)',
    obs.max_value AS MAX(obs.value) COMMENT = 'Maximum value (filter to one observation_type)'
)

COMMENT = 'OLIDS Clinical Observations History Semantic View - every recorded biomarker observation event; a person can have multiple readings of one biomarker on the same date. observation_event_id is the deterministic tiebreaker. Use for serial, latest-N, trajectory, variability, and recheck-interval analysis. Long format: filter to one observation_type before reading value.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE and keep person_id out of the final output. This view is observation-event grain, so keep the events: reduce to one row per person only when joining to a person-grain view. For latest-N, trajectory or change questions, hold clinical_effective_date and observation_event_id in the CTE and rank with ROW_NUMBER() in the OUTER query, filtering rn <= N there. filter one observation_type and clinical_effective_date window before linkage. Future-dated readings are excluded at source; legacy readings can be decades old, so date windows must state their range. Example: SELECT clinical_effective_date, AGG(patient_count) FROM SEM_OLIDS_OBSERVATIONS_HISTORY WHERE observation_type = ''HbA1c'' GROUP BY clinical_effective_date. Example linkage: reduce latest HbA1c values here and medication exposure in sem_olids_prescribing before joining. For latest-N, first isolate a person-grain CTE over this view (no window functions inside the semantic-view query — they do not compile there), then rank the CTE result in the OUTER query with ROW_NUMBER() OVER (PARTITION BY person_id, observation_type ORDER BY clinical_effective_date DESC, observation_event_id DESC). For change/trajectory questions, take each person''s latest reading and a baseline reading on or before DATEADD(month, -N, latest date), require both, then difference. Blood pressure is two observation_types: Systolic BP and Diastolic BP. RETENTION: full history is kept for currently-registered persons but only ~5 years for left/deceased persons — per-person trajectories in the current cohort are fine, but do not treat population cross-sections older than 5 years as representative (survivor bias). Use sem_olids_observations for current latest values.'
AI_QUESTION_CATEGORIZATION 'Use this view for: serial biomarker readings, latest-2 / last-N values, trends in an individual or cohort over time, variability, rate of change, time between readings, and recheck intervals. Frailty rows are GP-recorded eFI/eFI2 or Rockwood assessments; calculated eFI2 has no history and is only in sem_olids_observations. For the single most recent value per biomarker (current state) use sem_olids_observations. For condition prevalence/demographics use sem_olids_population. For condition incidence/prevalence trends use sem_olids_trends. Questions needing cohorts from TWO domains (e.g. biomarker trajectory x medication exposure) are answerable by joining this view to the other sem_olids_* views on person_id in CTEs, with aggregate-only output.'
