{{
    config(
        materialized='table',
        tags=['covid_flu'],
        cluster_by=['programme_type', 'campaign_id', 'practice_code', 'person_id']
    )
}}

/*
COVID and Flu Dashboard Base Table - LEGACY VIEW - TO BE REPLACED WITH A WIDE FORMAT

This model provides dashboard-ready data by combining the pure uptake facts
with demographics and practice information for the COVID and Flu Dashboard.

Key features:
- Joins fct_covid_flu_uptake with dim_person_demographics
- Provides complete demographic, geographic and practice dimensions
- Optimised clustering for dashboard query patterns
- Single source of truth for COVID/Flu dashboard consumption

Multi-Programme Support:
COVID Campaigns:
- COVID Autumn 2024, COVID Spring 2025, COVID Autumn 2025, COVID Spring 2026,
  COVID Autumn 2026

Flu Campaigns: 
- Flu 2024-25, Flu 2025-26, Flu 2026-27

Usage:
- Primary table for COVID and Flu Dashboard in PowerBI/Tableau
- Filter by programme_type for programme-specific dashboards
- Use campaign_id for specific campaign analysis
- Demographic breakdowns for equity analysis

PowerBI
eligible reason not required for dashboard. Reduce repeating rows
*/

WITH uptake_with_demographics AS (
    SELECT 
        -- Programme and campaign identifiers
        u.programme_type,
        CASE WHEN u.campaign_id = 'COVID Autumn 2024' THEN 'CV Autumn 2024'
         WHEN u.campaign_id = 'COVID Spring 2025' THEN 'CV Spring 2025'
         WHEN u.campaign_id = 'COVID Autumn 2025' THEN 'CV Autumn 2025'
         WHEN u.campaign_id = 'COVID Spring 2026' THEN 'CV Spring 2026'
         WHEN u.campaign_id = 'COVID Autumn 2026' THEN 'CV Autumn 2026'
         WHEN u.campaign_id = 'COVID Spring 2027' THEN 'CV Spring 2027'
         ELSE u.campaign_id END AS campaign_id,
        CASE 
        WHEN u.campaign_id = 'Flu 2024-25' THEN 1
        WHEN u.campaign_id = 'COVID Autumn 2024' THEN 2
        WHEN u.campaign_id = 'COVID Spring 2025' THEN 3
        WHEN u.campaign_id = 'Flu 2025-26' THEN 4
        WHEN u.campaign_id = 'COVID Autumn 2025' THEN 5
        WHEN u.campaign_id = 'COVID Spring 2026' THEN 6
        WHEN u.campaign_id = 'Flu 2026-27' THEN 7
        WHEN u.campaign_id = 'COVID Autumn 2026' THEN 8
        WHEN u.campaign_id = 'COVID Spring 2027' THEN 9
        END AS campaign_sort,
        u.campaign_year,
        u.campaign_season,
        u.person_id,
        id.hx_flake,
        
        -- Demographics from dim_person_demographics
        d.is_active,
        d.gender,
        d.age,
        d.age_band_5y,
        d.age_band_10y,
        d.age_band_nhs,
        d.ethnicity_category,
        CASE 
        WHEN d.ETHNICITY_CATEGORY = 'Asian' THEN 1
        WHEN d.ETHNICITY_CATEGORY = 'Black' THEN 2
        WHEN d.ETHNICITY_CATEGORY = 'Mixed' THEN 3
        WHEN d.ETHNICITY_CATEGORY = 'Other' THEN 4
        WHEN d.ETHNICITY_CATEGORY = 'White' THEN 5
        WHEN d.ETHNICITY_CATEGORY = 'Unknown' THEN 6
        END AS ethcat_order, 
        CASE
        WHEN d.ETHNICITY_SUBCATEGORY in ('Not Recorded','Not stated','Not Stated','Recorded Not Known','Refused') THEN 'Unknown'
        ELSE d.ETHNICITY_SUBCATEGORY END AS ethnicity_subcategory,
        CASE
        WHEN d.ethnicity_granular = 'MENA' THEN 'Middle East and North African'
        WHEN d.ethnicity_granular in ('Muslim', 'Sikh') THEN 'Unknown'
        WHEN d.ethnicity_granular in ('Recorded Not Known', 'Refused', 'Not stated', 'Not Recorded','Not Stated') THEN 'Unknown'
        WHEN d.ethnicity_granular = 'Black - E.African Asian' THEN 'East African Asian'
        WHEN d.ethnicity_granular = 'Indo-Caribbean' THEN 'Caribbean Asian'
        WHEN d.ethnicity_granular = 'Cornish' THEN 'English'
        WHEN d.ethnicity_granular in ('Gypsy or Irish Traveller','Gypsy','Irish Traveller') THEN 'Gypsy or Irish Traveller'
        WHEN d.ethnicity_granular in ('Albanian/Serbian', 'Serbian') THEN 'Albanian or Serbian'
        ELSE d.ethnicity_granular END AS ethnicity_granular,
        CASE 
        WHEN d.main_language = 'Pushto' THEN 'Pashto' 
        WHEN d.main_language = 'Gujerati' THEN 'Gujarati'
        WHEN d.main_language ILIKE '%sign language%' THEN 'Sign language'
        WHEN d.main_language = 'Norwegian Bokmål' THEN 'Norwegian'
        WHEN d.main_language = 'Not Recorded' THEN 'Unknown'
        ELSE d.main_language END AS main_language,
        d.language_type,
        d.interpreter_needed,
        d.interpreter_type,
        
        -- Geographic and deprivation (residence-based)
        d.imd_quintile_25,
        d.imd_decile_25,
        CASE 
        WHEN d.IMD_QUINTILE_25 = 'Most Deprived' THEN 1
        WHEN d.IMD_QUINTILE_25 = 'Second Most Deprived' THEN 2
        WHEN d.IMD_QUINTILE_25 = 'Third Most Deprived' THEN 3
        WHEN d.IMD_QUINTILE_25 = 'Second Least Deprived' THEN 4
        WHEN d.IMD_QUINTILE_25 = 'Least Deprived' THEN 5
        ELSE 6 END AS imdquintile_order,
        d.lsoa_code_21 as lsoa_code,
        d.lsoa_name_21 as lsoa_name,
        d.ward_code,
        d.ward_name,
        COALESCE(la.LAD25_NM,'Unknown') AS borough_resident,
        CASE WHEN la.RESIDENT_FLAG IS NULL THEN 'Unknown'
        ELSE la.RESIDENT_FLAG END as residential_loc,
        d.neighbourhood_resident,
        d.icb_code_resident,
        d.icb_resident,

        -- Practice information (registration-based)
        d.practice_code,
        d.practice_name,
        d.pcn_code,
        d.pcn_name,
        d.borough_registered,
        d.neighbourhood_registered,
        
        -- School information from dim_person_age (Early Years for ages 2-3, Reception+ for school ages)
        pa.age_school_stage AS school_year,
        pa.is_early_years_age,
        pa.is_primary_school_age,
        pa.is_secondary_school_age,

        -- Flu vaccination setting (Early Years at GP for ages 2-3, School-based for Reception-Year 11)
        CASE
            WHEN u.programme_type = 'FLU' THEN
                CASE
                    WHEN pa.age >= 2 AND pa.age < 4 THEN 'Early Years (GP)'  -- Ages 2-3
                    WHEN pa.age_school_stage IN ('Reception', 'Year 1', 'Year 2', 'Year 3', 'Year 4',
                                                  'Year 5', 'Year 6', 'Year 7', 'Year 8', 'Year 9',
                                                  'Year 10', 'Year 11') THEN 'School-based'
                    ELSE NULL  -- Year 12, Year 13, and older
                END
            ELSE NULL
        END AS flu_vaccination_setting,

        -- Housebound status from dim_person_housebound_status
        COALESCE(hs.is_housebound, FALSE) AS is_housebound,
        
        -- Eligibility information from uptake facts
        u.is_eligible,
        u.campaign_category,
        u.risk_group,
        u.subcohort,
        --u.eligibility_reason,
        --u.rule_type,

        -- Clinical risk group flags, any age, bounded to the campaign
        -- (int_covid_flu_risk_group_flags). A person aged 65 or over with CKD carries
        -- has_ckd = 1 on their age-based row.
        COALESCE(f.has_asthma, 0) AS has_asthma,
        COALESCE(f.has_asplenia, 0) AS has_asplenia,
        COALESCE(f.has_chd, 0) AS has_chd,
        COALESCE(f.has_ckd, 0) AS has_ckd,
        COALESCE(f.has_cld, 0) AS has_cld,
        COALESCE(f.has_cnd, 0) AS has_cnd,
        COALESCE(f.has_crd, 0) AS has_crd,
        COALESCE(f.has_diabetes, 0) AS has_diabetes,
        COALESCE(f.is_immunosuppressed, 0) AS is_immunosuppressed,
        COALESCE(f.has_ld, 0) AS has_ld,
        COALESCE(f.has_smi, 0) AS has_smi,
        COALESCE(f.in_clinical_risk_group, 0) AS in_clinical_risk_group,
        
        -- Vaccination information from uptake facts
        u.vaccination_status,
        DATE(u.vaccination_date) AS vaccination_date,
        TO_CHAR(TO_DATE(vaccination_date), 'MMMM') as vaccination_month,
        YEAR(vaccination_date) AS vaccination_year,
        u.vaccination_status_reason,
        u.vaccinated_despite_ineligible,
        
        -- Uptake flags and metrics from uptake facts
        u.vaccinated,
        u.declined,
        u.eligible_no_record,
        u.uptake_category,
        u.days_to_vaccination,
        
        -- Programme-specific fields
        u.laiv_given,
        
        -- Campaign dates
        u.campaign_start_date,
        u.campaign_reference_date,
        u.audit_end_date,
        u.created_at
        
    FROM {{ ref('fct_covid_flu_uptake') }} u
    LEFT JOIN {{ ref('int_covid_flu_risk_group_flags') }} f
        ON f.programme_type = u.programme_type
        AND f.campaign_id = u.campaign_id
        AND f.person_id = u.person_id
    LEFT JOIN {{ ref('dim_person_demographics') }} d
        ON u.person_id = d.person_id
    LEFT JOIN {{ ref('person_pseudo') }} id  
        ON u.person_id = id.person_id
    LEFT JOIN {{ ref('dim_person_age') }} pa
        ON u.person_id = pa.person_id
    LEFT JOIN {{ ref('dim_person_housebound_status') }} hs
        ON u.person_id = hs.person_id
    LEFT JOIN {{ ref('stg_reference_lsoa21_ward25_lad25') }} la 
        on la.LSOA21_CD = d.LSOA_CODE_21
    -- Only campaigns that had closed by the 2026-06-30 population snapshot. Measuring a
    -- later campaign against a June 2026 population would understate its uptake, and this
    -- predicate keeps that true without an edit each time a season is added.
    WHERE u.campaign_reference_date <= '2026-06-30'::DATE
)

SELECT distinct * FROM uptake_with_demographics
ORDER BY programme_type, campaign_id, practice_code, person_id