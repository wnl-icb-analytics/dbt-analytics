/*
COVID Chronic Respiratory Disease Eligibility Rule

Business Rule: Person is eligible if they have:
1. Asthma eligibility (via int_covid_asthma model), OR
2. Other chronic respiratory disease diagnosis (RESP_COV_COD) - any time
3. AND aged 5+ years (minimum age for COVID vaccination)
4. Only eligible in 2024/25 campaigns (broader eligibility)

Combination rule - includes asthma patients plus other respiratory conditions.
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

-- Step 1: Get asthma eligible people (already processed through complex logic)
people_with_asthma_eligibility AS (
    SELECT 
        campaign_id,
        person_id,
        qualifying_event_date,
        reference_date,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        'Asthma' AS respiratory_condition_type
    FROM {{ ref('int_covid_asthma') }}
),

-- Step 2: Find people with other chronic respiratory disease diagnosis (for all campaigns)
people_with_other_respiratory_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_resp_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'RESP_COV_COD'", 'UKHSA_COVID', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date,
        cc.campaign_reference_date
),

-- Step 3: Add age information for other respiratory conditions
people_with_other_respiratory_with_age AS (
    SELECT 
        pord.campaign_id,
        pord.person_id,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(pord.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        FLOOR(MONTHS_BETWEEN(pord.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        pord.first_resp_date AS qualifying_event_date,
        pord.campaign_reference_date,
        'Other chronic respiratory disease' AS respiratory_condition_type
    FROM people_with_other_respiratory_diagnosis pord
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pord.person_id = demo.person_id
    WHERE demo.birth_date_approx IS NOT NULL
        AND demo.birth_date_approx <= DATEADD('year', -5, pord.campaign_reference_date)  -- Minimum age 5, tested on birth date
),

-- Step 4: Combine asthma and other respiratory conditions (avoid duplicates)
all_respiratory_conditions AS (
    -- Include all asthma patients
    SELECT 
        campaign_id, person_id, qualifying_event_date, reference_date,
        birth_date_approx, age_months_at_ref_date, age_years_at_ref_date, respiratory_condition_type
    FROM people_with_asthma_eligibility
    
    UNION
    
    -- Include other respiratory conditions (exclude if already captured by asthma)
    SELECT 
        pord.campaign_id, pord.person_id, pord.qualifying_event_date, pord.campaign_reference_date AS reference_date,
        pord.birth_date_approx, pord.age_months_at_ref_date, pord.age_years_at_ref_date, 
        pord.respiratory_condition_type
    FROM people_with_other_respiratory_with_age pord
    WHERE NOT EXISTS (
        SELECT 1 FROM people_with_asthma_eligibility pae
        WHERE pae.campaign_id = pord.campaign_id AND pae.person_id = pord.person_id
    )
),

-- Step 5: Remove duplicates and get best qualifying event per person
best_respiratory_eligibility AS (
    SELECT 
        campaign_id, person_id, qualifying_event_date, reference_date,
        birth_date_approx, age_months_at_ref_date, age_years_at_ref_date, respiratory_condition_type,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY qualifying_event_date DESC, respiratory_condition_type
        ) AS rn
    FROM all_respiratory_conditions
),

-- Step 6: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Chronic Respiratory Disease' AS risk_group,
        person_id,
        qualifying_event_date,
        reference_date,
        CONCAT('Chronic respiratory disease: ', respiratory_condition_type) AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM best_respiratory_eligibility
    WHERE rn = 1  -- Only best eligibility per person
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id