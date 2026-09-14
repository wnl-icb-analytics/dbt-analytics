/*
COVID Homelessness Eligibility Rule

Business Rule: Person is eligible if either:
1. Homeless status or accommodation code (HOMELESS_COD) that is no older than their
   latest residence code (HOMELESS_DAT >= RESIDE_DAT), OR
2. They are actively registered at Camden Health Improvement Practice (Y02674), a
   homeless-specialist practice
3. AND aged 5+ years (minimum age for COVID vaccination)
4. Only eligible in 2024/25 campaigns (broader eligibility)

Recent homelessness status - within 2 years of campaign reference date.
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

-- Step 1: Find people with homeless status (for all campaigns)
people_with_homeless_status AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_homeless_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'HOMELESS_COD'", 'UKHSA_COVID', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_homeless = TRUE
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date,
        cc.campaign_reference_date
),

-- Step 2: Find people with residence codes (for comparison)
people_with_residence_status AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_residence_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'RESIDE_COD'", 'UKHSA_COVID', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        AND cc.eligible_homeless = TRUE
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date, cc.campaign_reference_date
),

-- Step 3: Apply homeless business logic (homeless date must be >= residence date)
people_with_valid_homeless_status AS (
    SELECT 
        phs.campaign_id,
        phs.person_id,
        phs.latest_homeless_date,
        prs.latest_residence_date,
        phs.campaign_reference_date,
        -- Homeless logic: IF HOMELESS_DAT ≥ RESIDE_DAT: Select
        CASE 
            WHEN prs.latest_residence_date IS NULL THEN TRUE  -- No residence code, homeless valid
            WHEN phs.latest_homeless_date >= prs.latest_residence_date THEN TRUE
            ELSE FALSE
        END AS is_currently_homeless
    FROM people_with_homeless_status phs
    LEFT JOIN people_with_residence_status prs
        ON phs.campaign_id = prs.campaign_id AND phs.person_id = prs.person_id
    WHERE phs.latest_homeless_date IS NOT NULL
),

-- Step 4: People actively registered at Camden Health Improvement Practice
registered_chip AS (
    SELECT person_id
    FROM {{ ref('dim_person_demographics') }}
    WHERE is_active = TRUE
        AND is_deceased = FALSE
        AND practice_code = 'Y02674'
),

-- Step 5: Combine both routes, one row per person per campaign
all_homeless_routes AS (
    SELECT
        pvhs.campaign_id,
        pvhs.person_id,
        pvhs.campaign_reference_date,
        pvhs.latest_homeless_date AS qualifying_event_date,
        TRUE AS via_homeless_code
    FROM people_with_valid_homeless_status pvhs
    WHERE pvhs.is_currently_homeless = TRUE

    UNION ALL

    -- CHIP registration: use the person's latest homeless code date where one exists
    SELECT
        cc.campaign_id,
        chip.person_id,
        cc.campaign_reference_date,
        -- Latest homeless code date where one exists, else the campaign start date
        -- (downstream models test qualifying_event_date as not null)
        COALESCE(phs.latest_homeless_date, cc.campaign_start_date) AS qualifying_event_date,
        FALSE AS via_homeless_code
    FROM registered_chip chip
    CROSS JOIN all_campaigns cc
    LEFT JOIN people_with_homeless_status phs
        ON phs.campaign_id = cc.campaign_id
        AND phs.person_id = chip.person_id
    WHERE cc.eligible_homeless = TRUE
),

eligible_people AS (
    SELECT
        campaign_id,
        person_id,
        campaign_reference_date,
        MAX(qualifying_event_date) AS qualifying_event_date,
        BOOLOR_AGG(via_homeless_code) AS via_homeless_code
    FROM all_homeless_routes
    GROUP BY campaign_id, person_id, campaign_reference_date
),

-- Step 6: Add age information and apply age restrictions
people_with_homeless_eligible_with_age AS (
    SELECT
        ep.campaign_id,
        ep.person_id,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(ep.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        FLOOR(MONTHS_BETWEEN(ep.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        ep.qualifying_event_date,
        ep.campaign_reference_date,
        ep.via_homeless_code
    FROM eligible_people ep
    LEFT JOIN {{ ref('dim_person_demographics') }} demo
        ON ep.person_id = demo.person_id
    WHERE demo.birth_date_approx IS NOT NULL
        -- Minimum age 5, tested on birth date (DATEDIFF('year') subtracts calendar years)
        AND demo.birth_date_approx <= DATEADD('year', -5, ep.campaign_reference_date)
),

-- Step 7: Format for eligibility table
final_eligible AS (
    SELECT
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Homelessness' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        CASE
            WHEN via_homeless_code THEN 'Latest residence status is homeless'
            ELSE 'Registered at Camden Health Improvement Practice'
        END AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_homeless_eligible_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id