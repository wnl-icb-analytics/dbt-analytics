/*
COVID Gestational Diabetes Eligibility Rule

Business Rule (spec 2.2 GDIAB_GROUP): Person is eligible if they have:
1. A gestational diabetes code (GDIAB_COD) on or after gestational_diabetes_start and on
   or before RUN_DAT (GDIAB_DAT, spec 2.4)
2. AND are in the pregnancy group for the campaign (int_covid_pregnancy)
3. Only computed for campaigns with eligible_gestational_diabetes (2024/25)

Feeds int_covid_diabetes, because DIAB_GROUP selects GDIAB_GROUP.

Pregnancy-specific diabetes that occurs during pregnancy.
This condition is NOT eligible in 2025/26 restricted campaigns.
*/

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='campaign_id',
    tags=['covid_flu']
) }}

WITH all_campaigns AS (
    -- Every COVID campaign the models report on
    -- (campaign list: macros/config/covid_campaign_selection.sql)
    {{ covid_build_campaigns() }}
),

-- Step 1: Find people with gestational diabetes diagnosis (for all campaigns)
people_with_gdm_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS gdm_date
    FROM ({{ get_observations("'GDIAB_COD'", 'UKHSA_COVID', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.gestational_diabetes_start
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_gestational_diabetes = TRUE
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2: Get pregnancy information (reuse COVID pregnancy logic)
pregnancy_eligible AS (
    SELECT 
        campaign_id,
        person_id,
        qualifying_event_date AS pregnancy_start_date,
        reference_date,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date
    FROM {{ ref('int_covid_pregnancy') }}
),

-- Step 3: Match gestational diabetes with current pregnancy
people_with_gdm_and_pregnancy AS (
    SELECT 
        pgdm.campaign_id,
        pgdm.person_id,
        pgdm.gdm_date,
        pe.pregnancy_start_date,
        pe.birth_date_approx,
        pe.age_months_at_ref_date,
        pe.age_years_at_ref_date,
        pe.reference_date AS campaign_reference_date
    FROM people_with_gdm_diagnosis pgdm
    INNER JOIN pregnancy_eligible pe 
        ON pgdm.campaign_id = pe.campaign_id AND pgdm.person_id = pe.person_id
),

-- Step 4: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Gestational Diabetes' AS risk_group,
        person_id,
        gdm_date AS qualifying_event_date,
        campaign_reference_date AS reference_date,
        'Gestational diabetes during pregnancy' AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_gdm_and_pregnancy
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id