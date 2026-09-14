/*
Flu Homeless Eligibility Rule

Business Rule: Person is eligible if either:
1. HOMELESS_GROUP - they have a homeless code (HOMELESS_COD) and that code is no older
   than their latest residence code (RESIDE_COD), i.e. HOMELESS_DAT >= RESIDE_DAT, OR
2. They are actively registered at Camden Health Improvement Practice (Y02674), a
   homeless-specialist practice
3. AND aged 16 years or older (minimum age for homeless flu vaccination)

HOMELESS_COD is a subset of RESIDE_COD, so the two clusters are compared as separate
latest dates rather than ranked in a single union - ranking ties every qualifying
observation with itself and picks a winner nondeterministically.
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

-- Step 1: Latest homeless code date per person (HOMELESS_DAT)
latest_homeless_date AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS homeless_date
    FROM ({{ get_observations("'HOMELESS_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2: Latest residence code date per person (RESIDE_DAT)
latest_residence_date AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS residence_date
    FROM ({{ get_observations("'RESIDE_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 3: Homeless code is current if it is not superseded by a later residence code
people_currently_homeless AS (
    SELECT
        h.campaign_id,
        h.person_id,
        h.homeless_date
    FROM latest_homeless_date h
    LEFT JOIN latest_residence_date r
        ON h.campaign_id = r.campaign_id
        AND h.person_id = r.person_id
    WHERE r.residence_date IS NULL
        OR h.homeless_date >= r.residence_date
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
        campaign_id,
        person_id,
        homeless_date AS qualifying_event_date,
        TRUE AS via_homeless_code
    FROM people_currently_homeless

    UNION ALL

    -- CHIP registration: use the person's latest homeless code date where one exists,
    -- else the campaign start date (downstream models test qualifying_event_date as not null)
    SELECT
        cc.campaign_id,
        chip.person_id,
        COALESCE(lhd.homeless_date, cc.campaign_start_date) AS qualifying_event_date,
        FALSE AS via_homeless_code
    FROM registered_chip chip
    CROSS JOIN all_campaigns cc
    LEFT JOIN latest_homeless_date lhd
        ON lhd.campaign_id = cc.campaign_id
        AND lhd.person_id = chip.person_id
),

eligible_people AS (
    SELECT
        campaign_id,
        person_id,
        MAX(qualifying_event_date) AS qualifying_event_date,
        BOOLOR_AGG(via_homeless_code) AS via_homeless_code
    FROM all_homeless_routes
    GROUP BY campaign_id, person_id
),

-- Step 6: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT
        ep.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Homeless' AS risk_group,
        ep.person_id,
        ep.qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        CASE
            WHEN ep.via_homeless_code THEN 'People who are homeless aged 16 or over'
            ELSE 'People who are homeless aged 16 or over (registered at Camden Health Improvement Practice)'
        END AS description,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM eligible_people ep
    JOIN all_campaigns cc
        ON ep.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON ep.person_id = demo.person_id
    WHERE 1=1
        -- Apply age restriction: 16 years or older (192 months)
        -- Aged 16 or over at RUN_DAT (spec 3.10 reports age bands 1, 6, 8 and 9;
        -- band 8 is 16 and over at RUN_DAT)
        AND DATEADD('year', 16, demo.birth_date_approx) <= cc.run_date
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id
