/*
Simplified Health and Social Care Worker Eligibility Rule

Business Rule: Person is eligible if they have:
1. ANY of the following worker codes (latest occurrence):
   - Care home worker (CAREHOME_COD)
   - Nursing home worker (NURSEHOME_COD)  
   - Domiciliary care worker (DOMCARE_COD)
2. AND aged 16 years or older (minimum age for health/social care worker flu vaccination)

Combination rule - multiple worker categories with OR logic.
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

-- Step 1: Find people with care home worker codes (for all campaigns)
people_with_care_home_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_carehome_date,
        'Care home worker' AS worker_type,
        cc.audit_end_date
    FROM ({{ get_observations("'CAREHOME_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Find people with nursing home worker codes (for all campaigns)
people_with_nursing_home_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_nursehome_date,
        'Nursing home worker' AS worker_type,
        cc.audit_end_date
    FROM ({{ get_observations("'NURSEHOME_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 3: Find people with domiciliary care worker codes (for all campaigns)
people_with_domcare_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_domcare_date,
        'Domiciliary care worker' AS worker_type,
        cc.audit_end_date
    FROM ({{ get_observations("'DOMCARE_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 4: Combine all health and social care worker evidence (for all campaigns)
all_hcworker_evidence AS (
    SELECT campaign_id, person_id, latest_carehome_date AS evidence_date, worker_type
    FROM people_with_care_home_codes
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_nursehome_date, worker_type
    FROM people_with_nursing_home_codes
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_domcare_date, worker_type
    FROM people_with_domcare_codes
),

-- Step 5: Get the most recent evidence per person (for all campaigns)
-- The spec reports care home, nursing home and domiciliary care workers as three separate
-- denominators. This model keeps one row per person, so worker_type carries the most
-- recent category, with the category name breaking a same-date tie so the choice is
-- deterministic. Splitting the three overlapping denominators would change the grain.
best_hcworker_evidence AS (
    SELECT 
        campaign_id,
        person_id,
        worker_type,
        evidence_date,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id, person_id
            ORDER BY evidence_date DESC, worker_type
        ) AS rn
    FROM all_hcworker_evidence
),

-- Step 5b: Workers already eligible by another route are excluded (spec indicators 21-23,
-- which reject ATRISK_GROUP, BMI_GROUP and PREG_GROUP before testing the worker codes)
people_eligible_via_other_routes AS (
    -- Clinical risk groups (ATRISK_GROUP)
    SELECT DISTINCT campaign_id, person_id
    FROM (
        SELECT campaign_id, person_id FROM {{ ref('int_flu_asthma') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_chronic_heart_disease') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_chronic_kidney_disease') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_chronic_liver_disease') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_diabetes') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_immunosuppression') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_chronic_neurological_disease') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_asplenia') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_chronic_respiratory_disease') }}
    )

    UNION

    -- BMI group
    SELECT DISTINCT campaign_id, person_id FROM {{ ref('int_flu_severe_obesity') }}

    UNION

    -- Pregnancy group
    SELECT DISTINCT campaign_id, person_id FROM {{ ref('int_flu_pregnancy') }}
),

-- Step 6: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        bhe.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Health and Social Care Workers' AS risk_group,
        bhe.person_id,
        bhe.worker_type,
        bhe.evidence_date AS qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'Health and social care workers aged 16 to under 65 and not eligible by another route' AS description,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM best_hcworker_evidence bhe
    JOIN all_campaigns cc
        ON bhe.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bhe.person_id = demo.person_id
    WHERE bhe.rn = 1  -- Only the most recent evidence per person
        -- Aged 16 or over at RUN_DAT and under 65 at REF_DAT (spec indicators 21-23).
        -- Tested on birth date, because DATEDIFF subtracts calendar parts rather than
        -- counting completed years.
        AND demo.birth_date_approx <= DATEADD('year', -16, cc.run_date)
        AND demo.birth_date_approx > DATEADD('year', -65, cc.campaign_reference_date)
        AND NOT EXISTS (
            SELECT 1
            FROM people_eligible_via_other_routes pevor
            WHERE pevor.campaign_id = bhe.campaign_id
              AND pevor.person_id = bhe.person_id
        )
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id