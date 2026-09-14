{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Vaccinations Semantic View
    ================================

    COVID and flu uptake is one row per person per campaign per risk group.
    Eligible people appear once per risk group; use COUNT(DISTINCT person_id),
    never COUNT(*) or sums of flags. Filter programme_type and campaign_id
    before calculating uptake. Filter is_eligible = TRUE for uptake so the
    vaccinated numerator excludes vaccination records without eligibility.

    Adult immunisation tables are current-state single-programme tables:
    pneumococcal is person grain, shingles is person x dose, and RSV is person
    grain. Population includes inactive and deceased people; filter is_active.
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics, geography, ethnicity, deprivation (current snapshot)',

    covid_flu AS {{ ref('fct_covid_flu_uptake') }}
        PRIMARY KEY (programme_type, campaign_id, person_id, risk_group)
        COMMENT = 'COVID and flu uptake by person, campaign and risk group',

    pneumo AS {{ ref('fct_pneumococcal_vaccination_status') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Pneumococcal vaccination status for people aged 65+ or in a PPV clinical risk group',

    shingles AS {{ ref('fct_shingles_vaccination_status') }}
        PRIMARY KEY (person_id, campaign)
        COMMENT = 'Shingles vaccination status by person and dose',

    rsv AS {{ ref('fct_rsv_vaccination_status') }}
        PRIMARY KEY (person_id)
        COMMENT = 'RSV vaccination status for the eligible cohort'
)

RELATIONSHIPS(
    covid_flu (person_id) REFERENCES demographics,
    pneumo (person_id) REFERENCES demographics,
    shingles (person_id) REFERENCES demographics,
    rsv (person_id) REFERENCES demographics
)

DIMENSIONS(
    -- Person linkage key
    demographics.person_id AS person_id COMMENT = 'Pseudonymised person key, shared by all sem_olids_* views. Exposed only for cross-view cohort intersection: join CTEs over two views on person_id, then aggregate. Never return person_id in final results.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for linkage to non-OLIDS views (SUS acute activity, cost index, resource index). Every active person has one; the underlying person-patient mapping can be many-to-many, so joins remain approximate at the margins. Join CTEs on sk_patient_id, then aggregate; never return sk_patient_id in final results.',

    -- COVID and flu uptake: one row per person per campaign per risk group
    covid_flu.programme_type AS programme_type COMMENT = 'Vaccination programme: COVID or FLU. Always filter with campaign_id before uptake calculations.',
    covid_flu.campaign_id AS campaign_id COMMENT = 'Programme-specific campaign identifier. Use an available campaign_id value and an explicit campaign or date period.',
    covid_flu.campaign_year AS campaign_year COMMENT = 'Campaign year, such as 2024/25',
    covid_flu.campaign_season AS campaign_season COMMENT = 'Campaign season: Autumn, Spring, or Annual.',
    covid_flu.risk_group AS risk_group COMMENT = 'Eligibility risk group. Eligible people can have more than one row, so use distinct-person metrics.',
    covid_flu.subcohort AS subcohort COMMENT = 'More detailed campaign subcohort within the eligibility risk group.',
    covid_flu.eligibility_reason AS eligibility_reason COMMENT = 'Recorded reason the person qualifies for the campaign.',
    covid_flu.campaign_category AS campaign_category COMMENT = 'Campaign eligibility category, or Not Eligible',
    covid_flu.uptake_category AS uptake_category COMMENT = 'Eligibility and vaccination outcome category',
    covid_flu.vaccination_status AS vaccination_status COMMENT = 'COVID/flu status: VACCINATION_ADMINISTERED, VACCINATION_DECLINED, NO_VACCINATION_RECORD, or LAIV_ADMINISTERED (flu only; counts as vaccinated).',
    covid_flu.vaccinated AS vaccinated COMMENT = 'Vaccinated flag. Flu LAIV counts as vaccinated.',
    covid_flu.declined AS declined COMMENT = 'Vaccination declined flag',
    covid_flu.eligible_no_record AS eligible_no_record COMMENT = 'Eligible with no vaccination record flag',
    covid_flu.is_eligible AS is_eligible COMMENT = 'Eligible for the selected programme and campaign',
    covid_flu.laiv_given AS laiv_given COMMENT = 'Live attenuated influenza vaccine given. FALSE for COVID.',
    covid_flu.vaccination_date AS vaccination_date COMMENT = 'Vaccination administration date',
    covid_flu.campaign_start_date AS campaign_start_date COMMENT = 'Start date defining the campaign scope.',
    covid_flu.campaign_reference_date AS campaign_reference_date COMMENT = 'Campaign reference date used for age and eligibility assessment.',
    covid_flu.audit_end_date AS audit_end_date COMMENT = 'End date defining the campaign audit scope. This does not make eligibility a historical snapshot.',
    covid_flu.vaccinated_despite_ineligible AS vaccinated_despite_ineligible COMMENT = 'Vaccinated despite no recorded eligibility',

    -- Adult immunisations
    pneumo.pneumococcal_status AS vaccination_status COMMENT = 'Current pneumococcal status; no campaign history is available.',
    pneumo.pneumococcal_vaccination_date AS vaccination_date COMMENT = 'Pneumococcal vaccination date',
    pneumo.in_ppv_clinical_risk_group AS in_ppv_clinical_risk_group COMMENT = 'In a PPV clinical risk group',
    shingles.shingles_dose AS campaign COMMENT = 'Shingles Dose 1 or Shingles Dose 2',
    shingles.shingles_status AS vaccination_status COMMENT = 'Current shingles dose status. One row per person per dose, so filter/group by campaign.',
    shingles.shingles_vaccination_date AS vaccination_date COMMENT = 'Shingles vaccination date',
    shingles.shingles_is_immunosuppressed AS is_immunosuppressed COMMENT = 'Immunosuppressed flag for the shingles cohort',
    shingles.shingles_turn_65_after_sep_2023 AS turn_65_after_sep_2023 COMMENT = 'Turned 65 after September 2023',
    rsv.rsv_status AS vaccination_status COMMENT = 'Current RSV status; no campaign history is available.',
    rsv.rsv_vaccination_date AS vaccination_date COMMENT = 'RSV vaccination date',
    rsv.rsv_is_care_home_resident AS is_care_home_resident COMMENT = 'Care home resident flag',
    rsv.rsv_is_pregnant AS is_pregnant COMMENT = 'Pregnancy flag for the maternity pathway',

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
    covid_flu.eligible_patients AS COUNT(DISTINCT CASE WHEN covid_flu.is_eligible THEN covid_flu.person_id END) COMMENT = 'Distinct people with is_eligible = TRUE. Filter programme_type and campaign_id first; risk groups fan out rows.',
    covid_flu.vaccinated_patients AS COUNT(DISTINCT CASE WHEN covid_flu.vaccinated THEN covid_flu.person_id END) COMMENT = 'Vaccinated people, including flu LAIV and people without recorded eligibility. For uptake, filter is_eligible = TRUE.',
    covid_flu.declined_patients AS COUNT(DISTINCT CASE WHEN covid_flu.declined THEN covid_flu.person_id END) COMMENT = 'People who declined vaccination',
    covid_flu.eligible_no_record_patients AS COUNT(DISTINCT CASE WHEN covid_flu.eligible_no_record THEN covid_flu.person_id END) COMMENT = 'Eligible people without a vaccination record',
    covid_flu.avg_days_to_vaccination AS AVG(covid_flu.days_to_vaccination) COMMENT = 'Average days from campaign reference date to vaccination',
    pneumo.pneumo_cohort_count AS COUNT(DISTINCT pneumo.person_id) COMMENT = 'Pneumococcal cohort: aged 65+ or PPV clinical risk group',
    pneumo.pneumo_vaccinated_count AS COUNT(DISTINCT CASE WHEN pneumo.vaccination_status = 'VACCINATION_ADMINISTERED' THEN pneumo.person_id END) COMMENT = 'Pneumococcal-vaccinated people',
    shingles.shingles_cohort_count AS COUNT(DISTINCT shingles.person_id) COMMENT = 'Shingles cohort: 70-79 catch-up, turned 65 after September 2023, or immunosuppressed aged 18+',
    shingles.shingles_dose_vaccinated_count AS COUNT(DISTINCT CASE WHEN shingles.vaccination_status = 'VACCINATION_ADMINISTERED' THEN shingles.person_id END) COMMENT = 'Vaccinated people for the selected shingles dose',
    rsv.rsv_cohort_count AS COUNT(DISTINCT rsv.person_id) COMMENT = 'RSV cohort: aged 75+, care home resident, or pregnant aged 12-55',
    rsv.rsv_vaccinated_count AS COUNT(DISTINCT CASE WHEN rsv.vaccination_status = 'VACCINATION_ADMINISTERED' THEN rsv.person_id END) COMMENT = 'RSV-vaccinated people'
)

COMMENT = 'OLIDS Vaccinations Semantic View - COVID and flu uptake by person, campaign and risk group; current-state pneumococcal, shingles and RSV vaccination status. COVID and flu are one row per person per campaign per risk group: headcounts must use COUNT(DISTINCT person_id), never COUNT(*) or flag sums. Pneumococcal and RSV are person grain; shingles is one row per person per DOSE (campaign = dose) - filter or group by campaign for shingles.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE, reduce to one row per person before joining on person_id, then aggregate; keep person_id out of the final output. COVID and flu are person-campaign-risk-group grain; filter programme_type and an available campaign_id value before calculating uptake and do not sum flags. For uptake, also filter is_eligible = TRUE so vaccinated_patients uses the eligible population; without that filter it includes vaccination-only records without recorded eligibility. State an explicit campaign or date period. Example: SELECT AGG(eligible_patients), AGG(vaccinated_patients) FROM SEM_OLIDS_VACCINATIONS WHERE programme_type = ''FLU'' AND campaign_id = ''Flu 2025-26'' AND is_eligible = TRUE. Example linkage: reduce flu patients here and diabetes people in sem_olids_population before joining. campaign_start_date, campaign_reference_date and audit_end_date define campaign scope; they do not reconstruct eligibility at an earlier date. For uptake as at a date, vaccination_date defines the numerator cutoff, but this view does not reconstruct the historical eligibility denominator. Flu LAIV counts as vaccinated. Pneumococcal, shingles and RSV are current-state programmes with no campaign history; shingles fans out one row per dose (campaign = dose), so filter or group by campaign there; cohorts: pneumococcal 65+ or PPV clinical risk group; shingles 70-79 catch-up, turned 65 after September 2023, or immunosuppressed 18+; RSV 75+, care home resident, or pregnant 12-55. Filter is_active = TRUE for coverage.'
AI_QUESTION_CATEGORIZATION 'Use this view for: COVID and flu vaccination uptake, eligibility and declines by campaign; pneumococcal, shingles and RSV vaccination status; and vaccination equity. Childhood immunisations are not included and will be in a future view.'
