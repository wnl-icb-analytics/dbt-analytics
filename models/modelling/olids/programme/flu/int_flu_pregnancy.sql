/*
Flu Pregnancy Eligibility Rule

Business Rule: Person is eligible if they have:
1. Scenario A: Pregnant on the campaign start date with no subsequent delivery, OR
2. Scenario B: Became pregnant between campaign start and audit end (remains eligible even if delivered)
3. AND aged 12 years or older (minimum age for pregnancy flu vaccination)

Campaign-specific logic:
- Scenario A: Latest pregnancy code in [1 January of the campaign year, campaign start),
  i.e. the 8 months before campaign start, with no subsequent delivery
- Scenario B: Pregnancy code between campaign_start_date and audit_end_date

Simplified rule - focuses on flu season timing rather than complex pregnancy state logic.
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

-- Step 1: Scenario B - Became pregnant during campaign period (for all campaigns)
people_pregnant_during_campaign AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_pregnancy_date,
        'Pregnant during flu campaign period' AS eligibility_reason,
        cc.audit_end_date
    FROM ({{ get_observations("'PREG_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.campaign_start_date
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Scenario A - Pregnant on the campaign start date with no subsequent delivery
-- Latest pregnancy code in the 8 months before campaign start (1 January to 31 August)
people_pregnant_at_campaign_start AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS pregnancy_date,
        DATEADD('month', -8, cc.campaign_start_date) AS pregnancy_eligibility_date,
        cc.audit_end_date
    FROM ({{ get_observations("'PREG_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        -- [1 January of the campaign year, campaign start)
        AND obs.clinical_effective_date >= DATEADD('month', -8, cc.campaign_start_date)
        AND obs.clinical_effective_date < cc.campaign_start_date
    GROUP BY cc.campaign_id, obs.person_id, cc.campaign_start_date, cc.audit_end_date
),

-- Step 3: Create lookup of pregnancy-only codes (to identify delivery/termination codes)
pregnancy_only_codes AS (
    SELECT DISTINCT spec_version, mapped_concept_code
    FROM ({{ get_observations("'PREG_COD'", 'UKHSA_FLU', versioned=true) }})
),

-- Step 4: Check for delivery codes after pregnancy date but before campaign start
people_with_delivery_after_pregnancy AS (
    SELECT DISTINCT
        pp.campaign_id,
        pp.person_id
    FROM people_pregnant_at_campaign_start pp
    JOIN all_campaigns cc
        ON pp.campaign_id = cc.campaign_id
    JOIN ({{ get_observations("'PREGDEL_COD'", 'UKHSA_FLU', versioned=true) }}) del_obs
        ON pp.person_id = del_obs.person_id
        AND del_obs.spec_version = cc.terminology_version
    LEFT JOIN pregnancy_only_codes poc
        ON del_obs.mapped_concept_code = poc.mapped_concept_code
        AND poc.spec_version = cc.terminology_version
    WHERE del_obs.clinical_effective_date > pp.pregnancy_date
        AND del_obs.clinical_effective_date < cc.campaign_start_date  -- Only before campaign start
        -- Delivery/termination codes are in PREGDEL_COD but NOT in PREG_COD
        AND poc.mapped_concept_code IS NULL
),

-- Step 5: Scenario A eligible people (pregnant at start, no subsequent delivery)
people_pregnant_at_start_no_delivery AS (
    SELECT 
        pp.campaign_id,
        pp.person_id,
        pp.pregnancy_date AS qualifying_event_date,
        'Pregnant at campaign start with no subsequent delivery' AS eligibility_reason,
        pp.audit_end_date
    FROM people_pregnant_at_campaign_start pp
    LEFT JOIN people_with_delivery_after_pregnancy pd
        ON pp.campaign_id = pd.campaign_id
        AND pp.person_id = pd.person_id
    WHERE pd.person_id IS NULL  -- No delivery found
),

-- Step 6: Combine all pregnancy eligibility paths (for all campaigns)
all_pregnancy_eligibility AS (
    -- Scenario A: Pregnant at campaign start with no subsequent delivery
    SELECT 
        campaign_id,
        person_id, 
        qualifying_event_date, 
        eligibility_reason,
        audit_end_date
    FROM people_pregnant_at_start_no_delivery
    
    UNION
    
    -- Scenario B: Became pregnant during campaign period
    SELECT 
        campaign_id,
        person_id, 
        latest_pregnancy_date AS qualifying_event_date, 
        eligibility_reason,
        audit_end_date
    FROM people_pregnant_during_campaign
),

-- Step 7: Remove duplicates and get best qualifying event per person (for all campaigns)
best_pregnancy_eligibility AS (
    SELECT 
        campaign_id,
        person_id,
        eligibility_reason,
        qualifying_event_date,
        audit_end_date,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY qualifying_event_date DESC, eligibility_reason
        ) AS rn
    FROM all_pregnancy_eligibility
),

-- Step 8: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        bpe.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Pregnancy' AS risk_group,
        bpe.person_id,
        bpe.qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'Women pregnant at flu campaign start or during campaign period' AS description,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        bpe.audit_end_date AS created_at
    FROM best_pregnancy_eligibility bpe
    JOIN all_campaigns cc
        ON bpe.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bpe.person_id = demo.person_id
    WHERE bpe.rn = 1  -- Only the best eligibility per person
        -- Apply age restrictions: 12 to under 65 years (144 months to under 65 years)
        -- PREG_GROUP carries no age rule in the spec; the search population floor is
        -- 6 months at RUN_DAT. The 12-year floor here is a local data-quality guard
        -- against miscoded pregnancy records, not a spec criterion.
        AND DATEADD('year', 12, demo.birth_date_approx) <= cc.run_date
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id