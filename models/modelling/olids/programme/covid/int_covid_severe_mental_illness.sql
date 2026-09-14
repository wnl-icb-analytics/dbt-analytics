/*
COVID Severe Mental Illness Eligibility Rule

Business Rule: Person is eligible if they have:
1. Severe mental illness diagnosis (SMI_COD) - any time in history
2. AND aged 5+ years (minimum age for COVID vaccination)
3. Computed for every campaign; the offer gate is applied in int_covid_under_65_at_risk

Simple diagnosis rule - any severe mental illness diagnosis qualifies.
Eligible in 2024/25 campaigns; not eligible in 2025/26.
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

-- Step 1: Find people with severe mental illness diagnosis (for all campaigns)
people_with_smi_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_smi_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'SEV_MENTAL_COD'", 'UKHSA_COVID', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY
        cc.campaign_id, obs.person_id, cc.audit_end_date, cc.campaign_reference_date
),

-- Step 2: Find people with resolved severe mental illness (for all campaigns)
people_with_smi_resolved AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_resolved_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'SMHRES_COD'", 'UKHSA_COVID', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date, cc.campaign_reference_date
),

-- Step 3: Apply resolved condition logic (SMI date > resolved date)
people_with_active_smi AS (
    SELECT 
        psmi.campaign_id,
        psmi.person_id,
        psmi.latest_smi_date,
        psmr.latest_resolved_date,
        psmi.campaign_reference_date,
        -- SMI logic: IF SEV_MENTAL_DAT > SMHRES_DAT: Select
        CASE 
            WHEN psmr.latest_resolved_date IS NULL THEN TRUE  -- No resolved code
            WHEN psmi.latest_smi_date > psmr.latest_resolved_date THEN TRUE
            ELSE FALSE
        END AS has_active_smi
    FROM people_with_smi_diagnosis psmi
    LEFT JOIN people_with_smi_resolved psmr
        ON psmi.campaign_id = psmr.campaign_id AND psmi.person_id = psmr.person_id
),

-- Step 4: Add age information and apply age restrictions  
people_with_smi_eligible_with_age AS (
    SELECT 
        pasmi.campaign_id,
        pasmi.person_id,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(pasmi.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        FLOOR(MONTHS_BETWEEN(pasmi.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        pasmi.latest_smi_date AS qualifying_event_date,
        pasmi.campaign_reference_date
    FROM people_with_active_smi pasmi
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pasmi.person_id = demo.person_id
    WHERE demo.birth_date_approx IS NOT NULL
        AND pasmi.has_active_smi = TRUE
        AND demo.birth_date_approx <= DATEADD('year', -5, pasmi.campaign_reference_date)  -- Minimum age 5, tested on birth date
),

-- Step 5: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Severe Mental Illness' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        'Severe mental illness (active, not resolved) - all campaigns' AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_smi_eligible_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id