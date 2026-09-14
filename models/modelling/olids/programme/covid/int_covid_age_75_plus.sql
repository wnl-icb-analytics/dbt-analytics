/*
COVID Age-Based Eligibility Rule

Business Rule: Person is eligible if they are:
1. Aged at least the campaign's age_based_min_age at the campaign reference date

The threshold is campaign-driven: the Autumn 2024 offer was 65 and over, later
campaigns are 75 and over. Age is tested on birth date rather than DATEDIFF, because
Snowflake DATEDIFF('year', ...) subtracts calendar years rather than counting
completed years.
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

-- Step 1: Find people at or over the campaign's age threshold at reference date
people_age_eligible AS (
    SELECT
        cc.campaign_id,
        cc.age_based_min_age,
        demo.person_id,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        cc.campaign_reference_date,
        cc.audit_end_date
    FROM {{ ref('dim_person_demographics') }} demo
    CROSS JOIN all_campaigns cc
    WHERE cc.eligible_age_75_plus = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND demo.birth_date_approx <= DATEADD('year', -cc.age_based_min_age, cc.campaign_reference_date)
),

-- Step 2: Format for eligibility table
final_eligible AS (
    SELECT
        campaign_id,
        'AGE_BASED' AS campaign_category,
        CASE WHEN age_based_min_age = 65 THEN 'Age 65 and Over' ELSE 'Age 75+' END AS risk_group,
        person_id,
        campaign_reference_date AS qualifying_event_date,
        campaign_reference_date AS reference_date,
        CASE
            WHEN age_based_min_age = 65 THEN 'Aged 65 years or over at campaign reference date'
            ELSE 'Aged 75 years or over at campaign reference date'
        END AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_age_eligible
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id