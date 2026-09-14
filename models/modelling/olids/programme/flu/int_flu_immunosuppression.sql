/*
Simplified Immunosuppression Eligibility Rule

Business Rule: Person is eligible if they have:
1. ANY of the following evidence of immunosuppression:
   - Immunosuppression diagnosis (IMMDX_COD) - latest occurrence
   - Immunosuppression medication (IMMRX_COD) since the six-month medication lookback date
   - Immunosuppression administration (IMM_ADM_COD) since the admin lookback date, which
     spec v16.0 widened from six months to three years before AUDITEND_DAT for 2026-27
   - Chemotherapy/radiotherapy (DXT_CHEMO_COD) since the six-month medication lookback date
2. AND aged 6 months or older (minimum age for flu vaccination)

Combination rule - multiple evidence sources with OR logic.
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

-- Step 1: Find people with immunosuppression diagnosis (for all campaigns)
people_with_immuno_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_diagnosis_date,
        'Immunosuppression diagnosis' AS evidence_type
    FROM ({{ get_observations("'IMMDX_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2: Find people with recent immunosuppression medications (for all campaigns)
people_with_recent_immuno_medications AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MAX(med.order_date) AS latest_medication_date,
        'Recent immunosuppression medication' AS evidence_type
    FROM ({{ get_medication_orders(cluster_id='IMMRX_COD', source='UKHSA_FLU', versioned=true) }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.spec_version = cc.terminology_version
        AND med.order_date IS NOT NULL
        AND med.order_date >= cc.immuno_medication_lookback_date
        AND med.order_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, med.person_id
),

-- Step 3: Find people with recent immunosuppression administration codes (for all campaigns)
-- IMMADM_DAT uses its own lookback: six months to 2025-26, three years from 2026-27
people_with_recent_immuno_admin AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_admin_date,
        'Recent immunosuppression administration' AS evidence_type
    FROM ({{ get_observations("'IMM_ADM_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.immuno_admin_lookback_date
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 4: Find people with recent chemotherapy/radiotherapy (for all campaigns)
people_with_recent_chemo AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_chemo_date,
        'Recent chemotherapy/radiotherapy' AS evidence_type
    FROM ({{ get_observations("'DXT_CHEMO_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.immuno_medication_lookback_date
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 5: Combine all immunosuppression evidence (for all campaigns)
all_immuno_evidence AS (
    SELECT campaign_id, person_id, latest_diagnosis_date AS evidence_date, evidence_type
    FROM people_with_immuno_diagnosis
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_medication_date, evidence_type
    FROM people_with_recent_immuno_medications
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_admin_date, evidence_type
    FROM people_with_recent_immuno_admin
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_chemo_date, evidence_type
    FROM people_with_recent_chemo
),

-- Step 6: Get the most recent evidence per person per campaign
best_immuno_evidence AS (
    SELECT 
        campaign_id,
        person_id,
        evidence_type,
        evidence_date,
        ROW_NUMBER() OVER (PARTITION BY campaign_id, person_id ORDER BY evidence_date DESC) AS rn
    FROM all_immuno_evidence
),

-- Step 7: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        bie.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Immunosuppression' AS risk_group,
        bie.person_id,
        bie.evidence_date AS qualifying_event_date,
        bie.evidence_type,
        cc.campaign_reference_date AS reference_date,
        'People with weakened immune systems or receiving immunosuppressive treatment' AS description,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM best_immuno_evidence bie
    JOIN all_campaigns cc ON bie.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bie.person_id = demo.person_id
    WHERE bie.rn = 1  -- Only the most recent evidence per person per campaign
        -- Apply age restrictions: 6 months to under 65 years
        AND DATEADD('month', 6, demo.birth_date_approx) <= cc.run_date
)

SELECT * FROM final_eligibility
ORDER BY person_id