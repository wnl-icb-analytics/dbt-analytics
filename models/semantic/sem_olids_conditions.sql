{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Conditions Detail Semantic View
    =====================================

    Person x condition register detail. Where sem_olids_population flattens
    conditions to boolean flags, this view keeps the register rows themselves:
    diagnosis dates (time since diagnosis, diagnosed-in-period cohorts) and
    historical episodes (on/off cycles, resolution).

    OLIDS is the One London Integrated Data Set — primary care data from
    system suppliers (currently EMIS Web, with TPP to follow), unified by the
    One London team.

    Grains (two fact tables — do NOT mix in one query):
    - ltc:      one row per person per condition currently on register
                (38 registers, QOF Business Rules v50)
    - episodes: one row per person per condition per episode, including
                resolved episodes and persons no longer on any register

    Population: registers include inactive and deceased persons — filter
    is_active = TRUE for current-population analysis.
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics, geography, ethnicity, deprivation (current snapshot)',

    ltc AS {{ ref('fct_person_ltc_summary') }}
        PRIMARY KEY (person_id, condition_code)
        COMMENT = 'One row per person per condition currently on register (38 registers, QOF v50), with earliest/latest diagnosis dates',

    episodes AS {{ ref('fct_person_condition_episodes') }}
        PRIMARY KEY (person_id, condition_name, episode_number)
        COMMENT = 'One row per person per condition per episode — historical on/off cycles without QOF restrictions. Includes resolved episodes.'
)

RELATIONSHIPS(
    ltc (person_id) REFERENCES demographics,
    episodes (person_id) REFERENCES demographics
)

FACTS(
    demographics.esp_weight AS esp_weight COMMENT = 'ESP 2013 population weight for this persons age band (out of 100,000 total). Use with age_band_esp for age-standardised rate calculation.',
    demographics.esp_proportion AS esp_proportion COMMENT = 'ESP 2013 weight as proportion (esp_weight / 100,000).'
)

DIMENSIONS(
    -- Person linkage key
    demographics.person_id AS person_id COMMENT = 'Pseudonymised person key, shared by all sem_olids_* views. Exposed only for cross-view cohort intersection: join CTEs over two views on person_id, then aggregate. Never return person_id in final results. Multimorbidity PAIRS: two CTEs over this view filtered to different condition_code values, joined on person_id.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for linkage to non-OLIDS views (SUS acute activity, cost index, resource index). Every active person has one; the underlying person-patient mapping can be many-to-many, so joins remain approximate at the margins. Join CTEs on sk_patient_id, then aggregate; never return sk_patient_id in final results.',

    -- Register rows (current registers)
    ltc.condition_code AS condition_code WITH SYNONYMS = ('condition', 'register code') COMMENT = 'Short condition code (AF, AST, CAN, CHD, CKD, COPD, CYP_AST, DEM, DEP, DM, EP, FH, FRAIL, GESTDIAB, HF, HTN, LD, LD_U14, NAF, NDH, OB, OST, PAD, PC, RA, SCD, SMI, STIA, THAL, ...). Filter this before counting.',
    ltc.condition_name AS condition_name COMMENT = 'Human-readable condition name',
    ltc.clinical_domain AS clinical_domain COMMENT = 'Clinical domain grouping (Cardiovascular, Respiratory, Mental Health, Metabolic, ...)',
    ltc.is_qof AS is_qof COMMENT = 'TRUE for a QOF Business Rules v50 register; FALSE for a locally-defined register.',
    ltc.earliest_diagnosis_date AS earliest_diagnosis_date WITH SYNONYMS = ('diagnosis date', 'diagnosed') COMMENT = 'Earliest qualifying diagnosis date for this register. For obesity (OB) this is the latest valid BMI date, not a diagnosis.',
    ltc.latest_diagnosis_date AS latest_diagnosis_date COMMENT = 'Latest qualifying diagnosis date for this register',

    -- Episode rows (historical on/off cycles)
    episodes.episode_condition_code AS condition_code COMMENT = 'Condition code on the episode row (37 codes; keyed on condition_name)',
    episodes.episode_condition_name AS condition_name COMMENT = 'Condition name on the episode row',
    episodes.episode_number AS episode_number COMMENT = 'Sequential episode index per person-condition (1 = first)',
    episodes.episode_status AS episode_status COMMENT = 'Episode state: active or resolved.',
    episodes.episode_start_date AS episode_start_date COMMENT = 'First onset/diagnosis event in the episode',
    episodes.episode_end_date AS episode_end_date COMMENT = 'Resolution date; NULL while active',
    episodes.first_ever_diagnosis_date AS first_ever_diagnosis_date COMMENT = 'Earliest episode start across all episodes of this condition for the person',
    episodes.total_episodes_for_condition AS total_episodes_for_condition COMMENT = 'Total episodes of this condition for the person',
    episodes.current_condition_status AS current_condition_status COMMENT = 'active/resolved on the LATEST episode row only; NULL on earlier rows — filter to it for current-state questions over episodes',

    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands (0-4, 5-9, ..., 80-84, 85+, Unknown)',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands (0-9, 10-19, ..., 70-79, 80+, Unknown)',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS Digital standard age bands (0-4, 5-14, 15-24, ..., 75-84, 85+)',
    demographics.age_band_esp AS age_band_esp COMMENT = 'ESP 2013 age bands. This view exposes esp_weight and esp_proportion as facts, so age-standardised rates can be built here directly; take ANY_VALUE(esp_proportion) per age band.',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (detailed groupings; Unknown/Not Stated/Not Recorded/Refused where missing)',
    demographics.main_language AS main_language COMMENT = 'Main spoken language (Not Recorded if unknown)',
    demographics.interpreter_needed AS interpreter_needed COMMENT = 'Whether interpreter is required',
    demographics.is_active AS is_active COMMENT = 'Currently registered with NCL GP practice. Registers include inactive/deceased persons — filter TRUE for current population.',
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

    -- Geography (residence)
    demographics.borough_resident AS borough_resident COMMENT = 'Residence borough',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Residence neighbourhood',
    demographics.ward_name AS ward_name COMMENT = 'Electoral ward 2025 name',

    -- Deprivation
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least). NULL if LSOA not mapped.',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)'
)

METRICS(
    -- Register metrics (filter condition_code / condition_name first)
    ltc.people_on_register AS COUNT(DISTINCT ltc.person_id) COMMENT = 'Distinct people on the selected register(s). Filter condition_code first; unfiltered this counts people on ANY register.',
    ltc.register_entries AS COUNT(ltc.person_id) COMMENT = 'Person-condition register rows (a person counts once per register)',

    -- Episode metrics (filter episode_condition_code / _name first)
    episodes.people_with_episodes AS COUNT(DISTINCT episodes.person_id) COMMENT = 'Distinct people with any episode of the selected condition(s)',
    episodes.episode_count AS COUNT(episodes.person_id) COMMENT = 'Episode rows',
    episodes.resolved_episode_count AS COUNT(CASE WHEN episodes.episode_status = 'resolved' THEN episodes.person_id END) COMMENT = 'Resolved episodes',
    episodes.active_episode_count AS COUNT(CASE WHEN episodes.episode_status = 'active' THEN episodes.person_id END) COMMENT = 'Active episodes',
    episodes.avg_episode_duration_days AS AVG(episodes.episode_duration_days) COMMENT = 'Average resolved-episode duration in days (NULL durations excluded)'
)

COMMENT = 'OLIDS Conditions Detail Semantic View - person x condition register rows with diagnosis dates (ltc, 38 registers QOF v50) and historical condition episodes (episodes). Enables time-since-diagnosis, diagnosed-in-period cohorts, resolution/episode analysis, and multimorbidity pairs via person_id self-joins. For flat boolean condition flags use sem_olids_population.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE, reduce to one row per person before joining on person_id, then aggregate; keep person_id out of the final output. Never mix ltc and episodes elements: ltc is current register grain and episodes is person-condition-episode grain. Filter condition_code before counting. Example: SELECT condition_code, AGG(people_on_register) FROM SEM_OLIDS_CONDITIONS WHERE condition_code = ''DM'' GROUP BY condition_code. For diagnosis cohorts use earliest_diagnosis_date (e.g. WHERE earliest_diagnosis_date >= DATEADD(year, -1, CURRENT_DATE) for newly diagnosed); obesity dates are BMI dates. Registers include inactive and deceased persons — filter is_active = TRUE unless asked otherwise. Multimorbidity pairs: two person-grain CTEs over this view, each filtered to one condition_code, joined on person_id. Example linkage: reduce a condition cohort here and orders in sem_olids_prescribing before joining. age_band_esp is available for grouping; esp_proportion is the ESP 2013 age-only weight in sem_olids_population. In-view COUNT(DISTINCT person_id) fails HERE with a granularity error (person_id is demographics grain; register and episode dimensions are finer) — use the governed metrics people_on_register or people_with_episodes for in-view headcounts and their HAVING suppression. COUNT(DISTINCT person_id) is still correct in the outer query over joined CTE results.'
AI_QUESTION_CATEGORIZATION 'Use this view for: when people were diagnosed, time since diagnosis, newly diagnosed cohorts by period, diagnosis-date-based incidence, condition episodes and resolution, recurrent episodes, and multimorbidity pair analysis. For current prevalence with boolean flags and demographics use sem_olids_population. For monthly prevalence/incidence trends use sem_olids_trends.'
