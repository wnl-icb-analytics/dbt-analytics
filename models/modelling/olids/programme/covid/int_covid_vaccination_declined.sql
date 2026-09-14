/*
COVID Vaccination Declined Rule

Business Rule: Person with COVID vaccination declined status if they have:
1. COVID vaccination declined code (COVDECL_COD) within tracking period
2. No age restrictions (applies to all ages)

This tracks people who actively declined vaccination.
Used for vaccination status reporting and understanding uptake barriers.
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

-- Step 1: Find people with COVID vaccination declined codes (for all campaigns)
people_with_covid_vaccination_declined AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_declined_date,
        cc.campaign_reference_date,
        cc.decline_tracking_start,
        cc.decline_tracking_end
    FROM ({{ get_observations("'COVDECL_COD'", 'UKHSA_COVID', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.decline_tracking_start
        AND obs.clinical_effective_date <= cc.decline_tracking_end
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.campaign_reference_date,
        cc.decline_tracking_start, cc.decline_tracking_end
),

-- Step 2: Add age information (for demographics, no age restrictions)
people_declined_with_age AS (
    SELECT 
        pcd.campaign_id,
        pcd.person_id,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(pcd.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        FLOOR(MONTHS_BETWEEN(pcd.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        pcd.latest_declined_date AS qualifying_event_date,
        pcd.campaign_reference_date
    FROM people_with_covid_vaccination_declined pcd
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pcd.person_id = demo.person_id
    WHERE demo.birth_date_approx IS NOT NULL
),

-- Step 3: Format for status table
final_declined AS (
    SELECT 
        campaign_id,
        'VACCINATION_DECLINED' AS campaign_category,
        'COVID Vaccination Declined' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        'COVID vaccination declined by patient' AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_declined_with_age
)

SELECT * FROM final_declined
ORDER BY campaign_id, person_id