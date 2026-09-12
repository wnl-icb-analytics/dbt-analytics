{#
    on_schema_change: sk_patient_id was added for cross-dataset linkage, and
    'fail' blocked the deploy outright. Appending lets a new column land, but
    existing person-months keep a NULL key until a full refresh backfills them.
    Both environments were fully refreshed on 2026-08-11, so the key is complete.
#}
{{
    config(
        materialized='incremental',
        unique_key=['person_id', 'analysis_month'],
        on_schema_change='append_new_columns',
        cluster_by=['analysis_month'],
        tags=['daily']
    )
}}

-- Person Month Analysis Base
-- Incremental table combining active person-months with demographics and conditions
-- Pre-applies temporal joins for fast analysis queries
-- 
-- Incremental Strategy:
-- - Processes only new months since last run
-- - Use `dbt run --full-refresh` to rebuild entire table
-- - Schedule periodic full refresh for late-arriving clinical data

WITH active_person_months AS (
    -- Generate person-months for registered patients
    -- Uses effective_end_date (accounts for death and deregistration) and registration
    -- date ranges to determine temporal activity, not the point-in-time registration_status
    SELECT DISTINCT
        ds.month_end_date as analysis_month,
        hr.person_id
    FROM {{ ref('dim_person_historical_practice') }} hr
    INNER JOIN {{ ref('int_date_spine') }} ds
        ON hr.registration_start_date <= ds.month_end_date
        AND (hr.effective_end_date IS NULL OR hr.effective_end_date >= ds.month_start_date)
        AND ds.month_end_date >= DATEADD('month', -60, CURRENT_DATE)  -- 5 year limit
        AND ds.month_end_date <= LAST_DAY(CURRENT_DATE)    -- Don't create future months
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ds.month_end_date, hr.person_id
        ORDER BY hr.is_current_registration DESC, hr.registration_start_date DESC
    ) = 1
)

SELECT 
    -- Core identifiers
    apm.analysis_month,
    apm.person_id,
    d.sk_patient_id,
    d.practice_name,
    
    -- Date components for filtering
    ds.year_number,
    ds.month_number,
    ds.quarter_number,
    
    -- Display labels
    ds.month_year_label,
    
    -- Financial year (NHS reporting)
    ds.financial_year_label as financial_year,
    ds.financial_year_start,
    ds.financial_quarter_label as financial_quarter,
    ds.financial_quarter_number,
    
    -- Demographics (from SCD-2 temporal join)
    d.birth_year,
    d.birth_date_approx,
    d.birth_date_approx_end_of_month,
    d.death_year,
    d.death_month,
    d.death_date_approx,
    d.is_deceased,
    d.gender,
    d.ethnicity_category,
    d.ethnicity_subcategory,
    d.ethnicity_granular,
    d.ethnicity_category_sort,
    d.ethnicity_display_sort_key,
    d.main_language,
    d.language_type,
    d.interpreter_type,
    d.interpreter_needed,
    d.is_active,
    d.inactive_reason,

    -- Age calculations for this analysis month
    {{ calculate_age_attributes(
        birth_date_field='d.birth_date_approx',
        reference_date_field='apm.analysis_month',
        birth_year_field='d.birth_year',
        birth_month_field='d.birth_month',
        is_deceased_field='d.is_deceased',
        death_date_field='d.death_date_approx'
    ) }},
    
    -- Practice and geography
    d.practice_code,
    d.borough_registered,
    d.sub_icb_code,
    d.sub_icb_name,
    d.practice_postcode,
    d.practice_lsoa,
    d.practice_msoa,
    d.practice_latitude,
    d.practice_longitude,
    d.neighbourhood_registered,
    d.pcn_code,
    d.pcn_name,
    d.pcn_name_with_borough,
    
    -- Address and household
    d.postcode_hash,
    d.uprn_hash,
    d.household_id,

    -- Residence geography (where they live)
    d.icb_code_resident,
    d.icb_resident,
    d.local_authority_code,
    d.local_authority_name,
    d.borough_resident,
    d.is_london_resident,
    d.london_classification,
    d.lsoa_code_21,
    d.lsoa_name_21,
    d.ward_code,
    d.ward_name,
    d.imd_decile_19,
    d.imd_quintile_19,
    d.imd_quintile_numeric_19,
    d.imd_decile_25,
    d.imd_quintile_25,
    d.imd_quintile_numeric_25,
    d.neighbourhood_resident,
    
    -- SCD2 metadata
    d.effective_start_date,
    d.effective_end_date,
    d.period_sequence,
    d.is_current,
    
    -- Condition flags (has_*)
    COALESCE(c.has_ast, FALSE) as has_ast,
    COALESCE(c.has_copd, FALSE) as has_copd,
    COALESCE(c.has_htn, FALSE) as has_htn,
    COALESCE(c.has_chd, FALSE) as has_chd,
    COALESCE(c.has_af, FALSE) as has_af,
    COALESCE(c.has_hf, FALSE) as has_hf,
    COALESCE(c.has_pad, FALSE) as has_pad,
    COALESCE(c.has_dm, FALSE) as has_dm,
    COALESCE(c.has_gestdiab, FALSE) as has_gestdiab,
    COALESCE(c.has_ndh, FALSE) as has_ndh,
    COALESCE(c.has_dep, FALSE) as has_dep,
    COALESCE(c.has_smi, FALSE) as has_smi,
    COALESCE(c.has_ckd, FALSE) as has_ckd,
    COALESCE(c.has_dem, FALSE) as has_dem,
    COALESCE(c.has_ep, FALSE) as has_ep,
    COALESCE(c.has_stia, FALSE) as has_stia,
    COALESCE(c.has_can, FALSE) as has_can,
    COALESCE(c.has_pc, FALSE) as has_pc,
    COALESCE(c.has_ld, FALSE) as has_ld,
    COALESCE(c.has_frail, FALSE) as has_frail,
    COALESCE(c.has_ra, FALSE) as has_ra,
    COALESCE(c.has_ost, FALSE) as has_ost,
    COALESCE(c.has_oa, FALSE) as has_oa,
    COALESCE(c.has_nafld, FALSE) as has_nafld,
    COALESCE(c.has_fh, FALSE) as has_fh,
    
    -- New episode flags (all conditions)
    COALESCE(c.new_ast, FALSE) as new_ast,
    COALESCE(c.new_copd, FALSE) as new_copd,
    COALESCE(c.new_htn, FALSE) as new_htn,
    COALESCE(c.new_chd, FALSE) as new_chd,
    COALESCE(c.new_af, FALSE) as new_af,
    COALESCE(c.new_hf, FALSE) as new_hf,
    COALESCE(c.new_pad, FALSE) as new_pad,
    COALESCE(c.new_dm, FALSE) as new_dm,
    COALESCE(c.new_gestdiab, FALSE) as new_gestdiab,
    COALESCE(c.new_ndh, FALSE) as new_ndh,
    COALESCE(c.new_dep, FALSE) as new_dep,
    COALESCE(c.new_smi, FALSE) as new_smi,
    COALESCE(c.new_ckd, FALSE) as new_ckd,
    COALESCE(c.new_dem, FALSE) as new_dem,
    COALESCE(c.new_ep, FALSE) as new_ep,
    COALESCE(c.new_stia, FALSE) as new_stia,
    COALESCE(c.new_can, FALSE) as new_can,
    COALESCE(c.new_pc, FALSE) as new_pc,
    COALESCE(c.new_ld, FALSE) as new_ld,
    COALESCE(c.new_frail, FALSE) as new_frail,
    COALESCE(c.new_ra, FALSE) as new_ra,
    COALESCE(c.new_ost, FALSE) as new_ost,
    COALESCE(c.new_oa, FALSE) as new_oa,
    COALESCE(c.new_nafld, FALSE) as new_nafld,
    COALESCE(c.new_fh, FALSE) as new_fh,
    
    -- Summary metrics
    COALESCE(c.total_active_conditions, 0) as total_active_conditions,
    COALESCE(c.total_new_episodes_this_month, 0) as total_new_episodes_this_month,
    COALESCE(c.has_any_condition, FALSE) as has_any_condition,
    COALESCE(c.has_any_new_episode, FALSE) as has_any_new_episode

FROM active_person_months apm

-- Join date spine for all date dimensions
INNER JOIN {{ ref('int_date_spine') }} ds
    ON apm.analysis_month = ds.month_end_date

-- Join demographics (temporal SCD-2 join)
INNER JOIN {{ ref('dim_person_demographics_historical') }} d
    ON apm.person_id = d.person_id
    AND apm.analysis_month >= d.effective_start_date
    AND (d.effective_end_date IS NULL OR apm.analysis_month < d.effective_end_date)

-- Condition flags from episodes table
LEFT JOIN (
    SELECT
        person_id,
        analysis_month,

        -- Active condition flags (has_*)
        MAX(CASE WHEN condition_code = 'AST' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_ast,
        MAX(CASE WHEN condition_code = 'COPD' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_copd,
        MAX(CASE WHEN condition_code = 'HTN' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_htn,
        MAX(CASE WHEN condition_code = 'CHD' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_chd,
        MAX(CASE WHEN condition_code = 'AF' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_af,
        MAX(CASE WHEN condition_code = 'HF' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_hf,
        MAX(CASE WHEN condition_code = 'PAD' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_pad,
        MAX(CASE WHEN condition_code = 'DM' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_dm,
        MAX(CASE WHEN condition_code = 'GESTDIAB' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_gestdiab,
        MAX(CASE WHEN condition_code = 'NDH' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_ndh,
        MAX(CASE WHEN condition_code = 'DEP' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_dep,
        MAX(CASE WHEN condition_code = 'SMI' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_smi,
        MAX(CASE WHEN condition_code = 'CKD' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_ckd,
        MAX(CASE WHEN condition_code = 'DEM' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_dem,
        MAX(CASE WHEN condition_code = 'EP' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_ep,
        MAX(CASE WHEN condition_code = 'STIA' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_stia,
        MAX(CASE WHEN condition_code = 'CAN' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_can,
        MAX(CASE WHEN condition_code = 'PC' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_pc,
        MAX(CASE WHEN condition_code = 'LD' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_ld,
        MAX(CASE WHEN condition_code = 'FRAIL' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_frail,
        MAX(CASE WHEN condition_code = 'RA' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_ra,
        MAX(CASE WHEN condition_code = 'OST' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_ost,
        MAX(CASE WHEN condition_code = 'OA' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_oa,
        MAX(CASE WHEN condition_code = 'NAFLD' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_nafld,
        MAX(CASE WHEN condition_code = 'FH' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_fh,
        MAX(CASE WHEN condition_code = 'ADHD' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_adhd,
        MAX(CASE WHEN condition_code = 'CLD' AND has_active_episode THEN 1 ELSE 0 END)::BOOLEAN as has_cld,

        -- New episode flags (new_*)
        MAX(CASE WHEN condition_code = 'AST' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_ast,
        MAX(CASE WHEN condition_code = 'COPD' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_copd,
        MAX(CASE WHEN condition_code = 'HTN' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_htn,
        MAX(CASE WHEN condition_code = 'CHD' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_chd,
        MAX(CASE WHEN condition_code = 'AF' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_af,
        MAX(CASE WHEN condition_code = 'HF' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_hf,
        MAX(CASE WHEN condition_code = 'PAD' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_pad,
        MAX(CASE WHEN condition_code = 'DM' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_dm,
        MAX(CASE WHEN condition_code = 'GESTDIAB' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_gestdiab,
        MAX(CASE WHEN condition_code = 'NDH' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_ndh,
        MAX(CASE WHEN condition_code = 'DEP' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_dep,
        MAX(CASE WHEN condition_code = 'SMI' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_smi,
        MAX(CASE WHEN condition_code = 'CKD' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_ckd,
        MAX(CASE WHEN condition_code = 'DEM' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_dem,
        MAX(CASE WHEN condition_code = 'EP' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_ep,
        MAX(CASE WHEN condition_code = 'STIA' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_stia,
        MAX(CASE WHEN condition_code = 'CAN' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_can,
        MAX(CASE WHEN condition_code = 'PC' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_pc,
        MAX(CASE WHEN condition_code = 'LD' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_ld,
        MAX(CASE WHEN condition_code = 'FRAIL' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_frail,
        MAX(CASE WHEN condition_code = 'RA' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_ra,
        MAX(CASE WHEN condition_code = 'OST' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_ost,
        MAX(CASE WHEN condition_code = 'OA' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_oa,
        MAX(CASE WHEN condition_code = 'NAFLD' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_nafld,
        MAX(CASE WHEN condition_code = 'FH' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_fh,
        MAX(CASE WHEN condition_code = 'ADHD' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_adhd,
        MAX(CASE WHEN condition_code = 'CLD' AND has_new_episode THEN 1 ELSE 0 END)::BOOLEAN as new_cld,

        -- Summary metrics
        COUNT(DISTINCT CASE WHEN has_active_episode THEN condition_code END) as total_active_conditions,
        COUNT(DISTINCT CASE WHEN has_new_episode THEN condition_code END) as total_new_episodes_this_month,
        (COUNT(DISTINCT CASE WHEN has_active_episode THEN condition_code END) > 0)::BOOLEAN as has_any_condition,
        (COUNT(DISTINCT CASE WHEN has_new_episode THEN condition_code END) > 0)::BOOLEAN as has_any_new_episode

    FROM (
        SELECT
            person_id,
            condition_code,
            ds.month_end_date as analysis_month,
            -- Active episode: ongoing during this month
            (episode_start_date <= ds.month_end_date
                AND (episode_end_date IS NULL OR episode_end_date >= ds.month_start_date))::BOOLEAN as has_active_episode,
            -- New episode: started this month
            (episode_start_date >= ds.month_start_date
                AND episode_start_date <= ds.month_end_date)::BOOLEAN as has_new_episode
        FROM {{ ref('fct_person_condition_episodes') }} ep
        CROSS JOIN {{ ref('int_date_spine') }} ds
        WHERE ds.month_end_date >= DATEADD('month', -60, CURRENT_DATE)  -- 5 years: limit based on complete left/died history
            AND ds.month_end_date <= LAST_DAY(CURRENT_DATE)
    ) episode_flags
    GROUP BY person_id, analysis_month
) c ON apm.person_id = c.person_id AND apm.analysis_month = c.analysis_month

{% if is_incremental() %}
    -- Only process new months since last run
    WHERE apm.analysis_month > (SELECT COALESCE(MAX(analysis_month), '1900-01-01') FROM {{ this }})
{% endif %}
