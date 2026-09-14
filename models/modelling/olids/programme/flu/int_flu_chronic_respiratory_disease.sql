/*
Simplified Chronic Respiratory Disease Eligibility Rule

Business Rule: Person is eligible if they have:
1. ANY of the following respiratory conditions:
   - Eligible via Asthma (AST_GROUP)
   - Chronic respiratory disease diagnosis (RESP_COD) - earliest occurrence
2. AND aged 6 months or older (minimum age for flu vaccination)

Combination rule - combines existing asthma eligibility with additional respiratory codes.
*/

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='campaign_id',
    tags=['covid_flu']
) }}

WITH all_campaigns AS (
    -- Every flu campaign the models report on
    -- (campaign list: macros/config/flu_campaign_selection.sql)
    {{ flu_build_campaigns() }}
),

-- Step 1: Get people eligible via asthma (for all campaigns)
people_eligible_via_asthma AS (
    SELECT 
        campaign_id,
        person_id,
        qualifying_event_date,
        'Eligible via asthma' AS eligibility_reason
    FROM {{ ref('int_flu_asthma') }}
),

-- Step 2: Find people with chronic respiratory disease diagnosis (for all campaigns)
people_with_chronic_resp_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_resp_date,
        'Chronic respiratory disease diagnosis' AS eligibility_reason,
        cc.audit_end_date
    FROM ({{ get_observations("'RESP_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 3: Combine all respiratory eligibility paths (for all campaigns)
all_respiratory_eligibility AS (
    SELECT campaign_id, person_id, qualifying_event_date, eligibility_reason
    FROM people_eligible_via_asthma
    
    UNION
    
    SELECT campaign_id, person_id, first_resp_date, eligibility_reason
    FROM people_with_chronic_resp_diagnosis
),

-- Step 4: Remove duplicates and get best qualifying event per person (for all campaigns)
best_respiratory_eligibility AS (
    SELECT 
        campaign_id,
        person_id,
        eligibility_reason,
        qualifying_event_date,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY qualifying_event_date DESC, eligibility_reason
        ) AS rn
    FROM all_respiratory_eligibility
),

-- Step 5: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        bre.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Chronic Respiratory Disease' AS risk_group,
        bre.person_id,
        bre.qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'People with chronic lung conditions (asthma, COPD, cystic fibrosis, etc.)' AS description,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM best_respiratory_eligibility bre
    JOIN all_campaigns cc
        ON bre.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bre.person_id = demo.person_id
    WHERE bre.rn = 1  -- Only the best eligibility per person
        -- Apply age restrictions: 6 months or older (minimum age for flu vaccination)
        AND DATEADD('month', 6, demo.birth_date_approx) <= cc.run_date
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id
