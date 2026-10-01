/*
COVID Vaccination Eligibility Fact Table

This model determines who is ELIGIBLE for COVID vaccination using clear,
individual rule models instead of complex macros.

Key improvements:
- Each rule is implemented in its own clear model
- Business logic is explicit and documented
- Terminology is descriptive
- Single configuration point for dates
- Direct use of core macros (get_observations, get_medication_orders)
- Works with multiple campaigns via covid_campaign_config macro
- Separate from vaccination status tracking (see fct_covid_status)

Which cohorts a campaign offers is read from covid_campaign_config(): the eligible_*
flags gate the clinical and other-risk groups (applied in the intermediates or in
int_covid_under_65_at_risk), immuno_max_age_years defines the separate under-75
immunosuppressed cohort, and care_home_min_age labels the care home cohort. No campaign
id is named here, so adding a season needs no edit to this model.

Multi-Campaign Support:
- COVID Autumn 2024: September 2024 - March 2025 (broader eligibility)
- COVID Spring 2025: April 2025 - June 2025 (restricted eligibility)
- COVID Autumn 2025: September 2025 - March 2026 (restricted eligibility)
- COVID Spring 2026: April 2026 - June 2026 (restricted eligibility)
- COVID Autumn 2026: September 2026 - March 2027 (restricted eligibility)

Usage:
- Default: Uses all defined COVID campaigns automatically
- Specific campaign analysis: Filter by campaign_id in downstream models
- For vaccination tracking, use fct_covid_status instead
- This replaces all the old complex macro-based models
- KH tidied eligible groups
*/

{{ config(
    materialized='table',
    tags=['covid_flu'],
    cluster_by=['campaign_id', 'person_id', 'campaign_category']
) }}

WITH all_campaigns AS (
    -- Every COVID campaign the models report on
    -- (campaign list: macros/config/covid_campaign_selection.sql)
    {{ covid_reported_campaigns() }}
),

-- Age-based eligibility. int_covid_age_75_plus applies age_based_min_age (65 for
-- Autumn 2024, 75 from Spring 2025).
age_based_eligibility AS (
    SELECT
        campaign_id, 'age_based' AS campaign_category, risk_group, NULL AS subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'AGE_BASED' AS rule_type, 1 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_age_75_plus') }}
),

clinical_condition_eligibility AS (
    -- Under 65 in a clinical risk group. int_covid_under_65_at_risk applies the
    -- eligible_* offer gate per group, so only campaigns that offered the clinical
    -- groups produce rows here.
    SELECT
        campaign_id, 'clinical_condition' AS campaign_category, 'Under 65 at risk' AS risk_group, risk_group AS subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_under_65_at_risk') }}

    UNION ALL

    -- Immunosuppressed under 75, the separate cohort of every offer from Spring 2025
    -- (spec Group M and predecessors). It exists only where the config caps it with
    -- immuno_max_age_years; in Autumn 2024 immunosuppression sat inside the under-65
    -- clinical groups above. The cap is tested on birth date.
    SELECT
        i.campaign_id, 'clinical_condition' AS campaign_category, 'Immunosuppressed under 75' AS risk_group, 'Immunosuppression' AS subcohort, i.person_id, i.qualifying_event_date, i.reference_date,
        i.description, i.birth_date_approx, i.age_months_at_ref_date, i.age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 2 AS eligibility_priority, i.created_at
    FROM {{ ref('int_covid_immunosuppression') }} i
    JOIN all_campaigns cc ON i.campaign_id = cc.campaign_id
    WHERE cc.eligible_immunosuppression
        AND cc.immuno_max_age_years IS NOT NULL
        AND i.birth_date_approx > DATEADD('year', -cc.immuno_max_age_years, i.reference_date)
),

-- Other risk groups. The homeless, pregnancy, care home and morbid obesity
-- intermediates apply their own eligible_* gate from the campaign config.
other_risk_eligibility AS (
    SELECT
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, NULL AS subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_homeless') }}

    UNION ALL

    SELECT
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, NULL AS subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_pregnancy') }}

    UNION ALL

    -- Long-term residential care, labelled with the campaign's minimum age
    -- (18 for Autumn 2024, 65 from Spring 2025).
    SELECT
        l.campaign_id, 'Other Risk Group' AS campaign_category, 'Long Term Residential Care ' || cc.care_home_min_age || '+' AS risk_group, NULL AS subcohort, l.person_id, l.qualifying_event_date, l.reference_date,
        l.description, l.birth_date_approx, l.age_months_at_ref_date, l.age_years_at_ref_date,
        'OTHER' AS rule_type, 2 AS eligibility_priority, l.created_at
    FROM {{ ref('int_covid_long_term_residential_care') }} l
    JOIN all_campaigns cc ON l.campaign_id = cc.campaign_id

    UNION ALL

    SELECT
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, NULL AS subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_morbid_obesity') }}
),

-- Union all eligibility types (vaccination tracking belongs in fct_covid_status)
all_eligibility AS (
    SELECT * FROM age_based_eligibility
    UNION ALL
    SELECT * FROM clinical_condition_eligibility
    UNION ALL
    SELECT * FROM other_risk_eligibility
),

-- Search population. Both specs require the patient to be registered for GMS at RUN_DAT.
-- int_covid_flu_campaign_population resolves that as at each campaign, so a closed season
-- keeps the people who were registered then rather than the people registered today.
registered_population AS (
    SELECT ae.*
    FROM all_eligibility ae
    JOIN {{ ref('int_covid_flu_campaign_population') }} pop
        ON pop.campaign_id = ae.campaign_id
        AND pop.person_id = ae.person_id
),

final_eligibility AS (
    SELECT
        campaign_id,
        campaign_category,
        risk_group,
        subcohort,
        person_id,
        qualifying_event_date,
        reference_date,
        description AS eligibility_reason,
        rule_type,
        eligibility_priority,
        birth_date_approx,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        created_at
    FROM registered_population
)

SELECT DISTINCT * FROM final_eligibility
ORDER BY person_id, eligibility_priority, campaign_category
