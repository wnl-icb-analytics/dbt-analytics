/*
Under-65 Clinical At-Risk Groups (offered cohort)

The ten UKHSA ATRISK_GROUP clinical groups for people under 65 at the campaign
reference date, limited to the campaigns whose offer included that group. The offer
gate is the eligible_* flag in covid_campaign_config(), applied here per group.

The clinical intermediates themselves compute every campaign, so that
int_covid_flu_risk_group_flags can report risk-group membership at any age in any
season. This model is where the offer is applied. It preserves each clinical group;
it does not create a synthetic "Under 65 At Risk" parent row.

Immunosuppression is one of the ten groups only where the offer has no separate
under-75 immunosuppressed cohort (immuno_max_age_years IS NULL, Autumn 2024). From
Spring 2025 that cohort is published on its own by fct_covid_eligibility.
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

clinical_risk_groups AS (
    SELECT
        s.campaign_id, s.campaign_category, s.risk_group, s.person_id, s.qualifying_event_date,
        s.reference_date, s.description, s.birth_date_approx, s.age_months_at_ref_date,
        s.age_years_at_ref_date, s.created_at
    FROM {{ ref('int_covid_chronic_heart_disease') }} s
    JOIN all_campaigns cc ON s.campaign_id = cc.campaign_id
    WHERE cc.eligible_chronic_heart_disease

    UNION ALL

    SELECT
        s.campaign_id, s.campaign_category, s.risk_group, s.person_id, s.qualifying_event_date,
        s.reference_date, s.description, s.birth_date_approx, s.age_months_at_ref_date,
        s.age_years_at_ref_date, s.created_at
    FROM {{ ref('int_covid_chronic_liver_disease') }} s
    JOIN all_campaigns cc ON s.campaign_id = cc.campaign_id
    WHERE cc.eligible_chronic_liver_disease

    UNION ALL

    SELECT
        s.campaign_id, s.campaign_category, s.risk_group, s.person_id, s.qualifying_event_date,
        s.reference_date, s.description, s.birth_date_approx, s.age_months_at_ref_date,
        s.age_years_at_ref_date, s.created_at
    FROM {{ ref('int_covid_chronic_neurological_disease') }} s
    JOIN all_campaigns cc ON s.campaign_id = cc.campaign_id
    WHERE cc.eligible_chronic_neurological_disease

    UNION ALL

    SELECT
        s.campaign_id, s.campaign_category, 'Asplenia' AS risk_group, s.person_id, s.qualifying_event_date,
        s.reference_date, s.description, s.birth_date_approx, s.age_months_at_ref_date,
        s.age_years_at_ref_date, s.created_at
    FROM {{ ref('int_covid_asplenia') }} s
    JOIN all_campaigns cc ON s.campaign_id = cc.campaign_id
    WHERE cc.eligible_asplenia

    UNION ALL

    SELECT
        s.campaign_id, s.campaign_category, s.risk_group, s.person_id, s.qualifying_event_date,
        s.reference_date, s.description, s.birth_date_approx, s.age_months_at_ref_date,
        s.age_years_at_ref_date, s.created_at
    FROM {{ ref('int_covid_chronic_kidney_disease') }} s
    JOIN all_campaigns cc ON s.campaign_id = cc.campaign_id
    WHERE cc.eligible_chronic_kidney_disease

    UNION ALL

    SELECT
        s.campaign_id, s.campaign_category, s.risk_group, s.person_id, s.qualifying_event_date,
        s.reference_date, s.description, s.birth_date_approx, s.age_months_at_ref_date,
        s.age_years_at_ref_date, s.created_at
    FROM {{ ref('int_covid_diabetes') }} s
    JOIN all_campaigns cc ON s.campaign_id = cc.campaign_id
    WHERE cc.eligible_diabetes

    UNION ALL

    SELECT
        s.campaign_id, s.campaign_category, s.risk_group, s.person_id, s.qualifying_event_date,
        s.reference_date, s.description, s.birth_date_approx, s.age_months_at_ref_date,
        s.age_years_at_ref_date, s.created_at
    FROM {{ ref('int_covid_immunosuppression') }} s
    JOIN all_campaigns cc ON s.campaign_id = cc.campaign_id
    WHERE cc.eligible_immunosuppression
        AND cc.immuno_max_age_years IS NULL

    UNION ALL

    SELECT
        s.campaign_id, s.campaign_category, s.risk_group, s.person_id, s.qualifying_event_date,
        s.reference_date, s.description, s.birth_date_approx, s.age_months_at_ref_date,
        s.age_years_at_ref_date, s.created_at
    FROM {{ ref('int_covid_chronic_respiratory_disease') }} s
    JOIN all_campaigns cc ON s.campaign_id = cc.campaign_id
    WHERE cc.eligible_chronic_respiratory_disease

    UNION ALL

    SELECT
        s.campaign_id, s.campaign_category, s.risk_group, s.person_id, s.qualifying_event_date,
        s.reference_date, s.description, s.birth_date_approx, s.age_months_at_ref_date,
        s.age_years_at_ref_date, s.created_at
    FROM {{ ref('int_covid_severe_mental_illness') }} s
    JOIN all_campaigns cc ON s.campaign_id = cc.campaign_id
    WHERE cc.eligible_severe_mental_illness

    UNION ALL

    SELECT
        s.campaign_id, s.campaign_category, s.risk_group, s.person_id, s.qualifying_event_date,
        s.reference_date, s.description, s.birth_date_approx, s.age_months_at_ref_date,
        s.age_years_at_ref_date, s.created_at
    FROM {{ ref('int_covid_learning_disability') }} s
    JOIN all_campaigns cc ON s.campaign_id = cc.campaign_id
    WHERE cc.eligible_learning_disability
),

under_65_clinical_risk_groups AS (
    SELECT *
    FROM clinical_risk_groups
    -- Under 65 at REF_DAT, tested on birth date
    WHERE birth_date_approx > DATEADD('year', -65, reference_date)
)

SELECT * FROM under_65_clinical_risk_groups
ORDER BY campaign_id, person_id, risk_group
