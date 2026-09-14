/*
Simplified Learning Disability Eligibility Rule

Business Rule: Person is eligible if they have:
1. A learning disability diagnosis (LEARNDIS_COD) - latest occurrence in history
2. AND aged 6 months or older (minimum age for flu vaccination)

Simple diagnosis rule - single code with age restrictions.
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

-- Step 1: Find people with learning disability diagnosis (for all campaigns)
people_with_learning_disability_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_learndis_date,
        cc.audit_end_date
    FROM ({{ get_observations("'LEARNDIS_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        ld.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Learning Disability' AS risk_group,
        ld.person_id,
        ld.latest_learndis_date AS qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'People with learning disabilities aged 6 months to 64 years' AS description,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        ld.audit_end_date AS created_at
    FROM people_with_learning_disability_diagnosis ld
    JOIN all_campaigns cc
        ON ld.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON ld.person_id = demo.person_id
    WHERE 1=1
        -- Apply age restrictions: 6 months to under 65 years
        AND DATEADD('month', 6, demo.birth_date_approx) <= cc.run_date
)

SELECT * FROM final_eligibility
ORDER BY person_id