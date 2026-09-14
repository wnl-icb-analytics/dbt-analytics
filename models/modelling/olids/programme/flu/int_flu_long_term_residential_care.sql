/*
Flu Long-term Residential Care Eligibility Rule

Business Rule: Person is eligible if they have:
1. LONGRES_GROUP - a long-term residential care code (LONGRES_COD) that is no older than
   their latest residence code (RESIDE_COD), i.e. LONGRES_DAT >= RESIDE_DAT
2. AND aged 6 months or over (no upper age limit)

UKHSA removed the long-stay residential care indicator and the LONGRES_COD group from
the flu specification at v15.7. The cohort is kept as a local extension, driven by the
eligible_long_term_residential_care flag in flu_campaign_config(), because care home
residents' vaccinations are coordinated through a provider and the cohort is still
needed for planning. It drives eligibility in fct_flu_eligibility like any other cohort.

LONGRES_COD is read from the COVID workbook, which still publishes it, without version
pinning: the cluster is the same seven codes in every COVID and flu version since 2024,
so nothing moves between seasons. RESIDE_COD is read from the flu workbook, pinned to
the campaign's version like every other cluster.

LONGRES_COD is a subset of RESIDE_COD, so the two clusters are compared as separate
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
    -- Campaigns that still report the long-stay residential care cohort
    -- (campaign list: macros/config/flu_campaign_selection.sql)
    SELECT *
    FROM (
        {{ flu_build_campaigns() }}
    )
    WHERE eligible_long_term_residential_care = TRUE
),

-- Step 1: Latest long-term care code date per person (LONGRES_DAT)
latest_longres_date AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS longres_date
    -- COVID workbook, current version: the flu workbook dropped this cluster at v5.8 and
    -- the codes have never changed, so no version pin is needed (see header)
    FROM ({{ get_observations("'LONGRES_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
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

-- Step 3: Long-term care code is current if not superseded by a later residence code
people_in_long_term_care AS (
    SELECT
        l.campaign_id,
        l.person_id,
        l.longres_date AS latest_residential_date
    FROM latest_longres_date l
    LEFT JOIN latest_residence_date r
        ON l.campaign_id = r.campaign_id
        AND l.person_id = r.person_id
    WHERE r.residence_date IS NULL
        OR l.longres_date >= r.residence_date
),

-- Step 4: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT
        pltc.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Long Term Residential Care' AS risk_group,
        pltc.person_id,
        pltc.latest_residential_date AS qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'People living in care homes or long-term residential care' AS description,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM people_in_long_term_care pltc
    JOIN all_campaigns cc
        ON pltc.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON pltc.person_id = demo.person_id
    WHERE 1=1
        -- Apply age restrictions: 6 months or over (no upper age limit)
        AND DATEADD('month', 6, demo.birth_date_approx) <= cc.run_date
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id
