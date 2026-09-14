/*
COVID Asplenia/Dysfunction of Spleen Eligibility Rule

Business Rule: Person is eligible if they have:
1. Asplenia or dysfunction of the spleen diagnosis (SPLN_COV_COD) - any time in history
2. AND aged 5+ years (minimum age for COVID vaccination)
3. Only eligible in 2024/25 campaigns (broader eligibility)

Simple diagnosis rule - any spleen dysfunction diagnosis qualifies.
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

-- Step 1: Find people with asplenia/spleen dysfunction diagnosis (for all campaigns)
people_with_spleen_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_spleen_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'SPLN_COV_COD'", 'UKHSA_COVID', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date, cc.campaign_reference_date
),

-- Step 2: Add age information and apply age restrictions  
people_with_spleen_eligible_with_age AS (
    SELECT 
        psd.campaign_id,
        psd.person_id,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(psd.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        FLOOR(MONTHS_BETWEEN(psd.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        psd.first_spleen_date AS qualifying_event_date,
        psd.campaign_reference_date
    FROM people_with_spleen_diagnosis psd
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON psd.person_id = demo.person_id
    WHERE demo.birth_date_approx IS NOT NULL
        AND demo.birth_date_approx <= DATEADD('year', -5, psd.campaign_reference_date)  -- Minimum age 5, tested on birth date
),

-- Step 3: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Asplenia/Spleen Dysfunction' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        'Asplenia or dysfunction of the spleen' AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_spleen_eligible_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id