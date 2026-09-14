/*
Under-65 Clinical At-Risk Groups

This model retains the eight UKHSA ATRISK_GROUP clinical groups for people
under 65 at the campaign reference date. It preserves each clinical group;
it does not create a synthetic "Under 65 At Risk" parent row.
*/

{{ config(materialized='table') }}

WITH clinical_risk_groups AS (
    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_flu_chronic_heart_disease') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_flu_chronic_liver_disease') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_flu_chronic_neurological_disease') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_flu_asplenia') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, 'Chronic Kidney Disease' AS risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_flu_chronic_kidney_disease') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_flu_diabetes') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_flu_immunosuppression') }}

    UNION ALL

    SELECT
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date,
        reference_date, description, birth_date_approx, age_months_at_ref_date,
        age_years_at_ref_date, created_at
    FROM {{ ref('int_flu_chronic_respiratory_disease') }}
),

under_65_clinical_risk_groups AS (
    SELECT *
    FROM clinical_risk_groups
    WHERE birth_date_approx > DATEADD('year', -65, reference_date)
)

SELECT * FROM under_65_clinical_risk_groups
ORDER BY campaign_id, person_id, risk_group
