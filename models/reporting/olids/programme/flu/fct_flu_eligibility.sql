/*
Flu Vaccination Eligibility Fact Table

This model determines who is ELIGIBLE for flu vaccination using clear, 
individual rule models instead of complex macros.

Key improvements:
- Each rule is implemented in its own clear model
- Business logic is explicit and documented
- Terminology is descriptive  
- Single configuration point for dates
- Direct use of core macros (get_observations, get_medication_orders)
- Covers every campaign in flu_reported_campaign_ids()
- Separate from vaccination status tracking (see fct_flu_status)

Flu Campaigns:
- Flu 2024-25, Flu 2025-26, Flu 2026-27

Usage: 
- Filter by campaign_id for a specific season
- For vaccination tracking, use fct_flu_status instead
- This replaces all the old complex macro-based models
- Rule/campaign category groups simplified and aligned.
*/

{{ config(
    materialized='table',
    tags=['covid_flu'],
    cluster_by=['campaign_id', 'person_id', 'campaign_category']
) }}

WITH
-- Age-based eligibility (both campaigns automatically included from intermediate models)
age_based_eligibility AS (
    -- Over 65
    SELECT 
        campaign_id, campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'AGE_BASED' AS rule_type, 1 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_over_65') }}
    --FROM MODELLING.OLIDS_PROGRAMME.INT_FLU_OVER_65
    
    UNION ALL
    
    -- Children preschool age
    SELECT 
        campaign_id, campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'AGE_BASED' AS rule_type, 1 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_children_preschool') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_flu_children_preschool
    
    UNION ALL
    
    -- Children school age
    SELECT 
        campaign_id, campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'AGE_BASED' AS rule_type, 1 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_children_school_age') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_flu_children_school_age
),

-- Simple clinical condition eligibility for Under 65 at Risk
clinical_condition_eligibility AS (
 -- under_65_at_risk
    SELECT 
        campaign_id, campaign_category, 'Under 65 at risk' as risk_group, risk_group as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_under_65_at_risk') }}
     --FROM MODELLING.OLIDS_PROGRAMME.int_flu_under_65_at_risk
    ),

-- Other non clinical risk eligibility for Under 65 at Risk
other_risk_eligibility AS ( 
 -- Health & Social Care Workers (Other rule)
    SELECT 
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_health_social_care_worker') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_flu_health_social_care_worker 
      
    UNION ALL

     -- Homeless (Other rule)
    SELECT 
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_homeless') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_flu_homeless

   UNION ALL

    -- Long-term Residential Care (Other rule)
    SELECT 
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_long_term_residential_care') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_flu_long_term_residential_care
    
    UNION ALL

     -- Pregnancy (Other rule)
    SELECT 
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_pregnancy') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_flu_pregnancy
    
    UNION ALL
    
    -- Carer (Other rule)
    SELECT 
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 5 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_carer') }}

    UNION ALL

    -- Household contact of an immunosuppressed person (spec indicator 19)
    SELECT
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 5 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_household_immunocompromised') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_flu_carer

     UNION ALL
    
    -- Severe Obesity (Other rule)
    SELECT 
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_severe_obesity') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_flu_severe_obesity

     UNION ALL
    
     -- Learning Disability (Other rule)
    SELECT 
        campaign_id, 'Other Risk Group' AS campaign_category, risk_group, null as subcohort, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'OTHER' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_learning_disability') }}
    --FROM MODELLING.OLIDS_PROGRAMME.int_flu_learning_disability 

   
),

-- Union all eligibility types (vaccination tracking removed - belongs in separate table)
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

-- Final formatting (campaign information already included in intermediate models)
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

SELECT * FROM final_eligibility
ORDER BY person_id, eligibility_priority, campaign_category
