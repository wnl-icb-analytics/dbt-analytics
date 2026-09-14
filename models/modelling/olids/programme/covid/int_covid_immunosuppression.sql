/*
COVID Immunosuppression Eligibility Rule

This implements RECALL_IMMUNO_GROUP, the group the campaign offer selects (spec 5.1.1
Group M and its predecessors), not IMMUNO_GROUP, which is the uptake monitoring variant.
The two differ only in where the medication, chemotherapy and admin lookbacks are anchored:
the recall group measures them from RUN_DAT, the uptake group from START_DAT.

Business Rule: Person is eligible if they have:
1. ANY of the following evidence of immunosuppression:
   - Immunosuppression diagnosis (IMMDX_COV_COD) - ever
   - Immunosuppression medication (IMMRX_COD) in the 6 months before RUN_DAT
   - Immunosuppression administration (IMM_ADM_COD) in the 3 years before RUN_DAT
   - Chemotherapy/radiotherapy (DXT_CHEMO_COD) in the 6 months before RUN_DAT
2. AND aged 6 months or over at the reference date

No upper age bound is applied here. The offer's under-75 cap (immuno_max_age_years,
Spring 2025 onward) is applied by fct_covid_eligibility, so this model can also feed
int_covid_flu_risk_group_flags with immunosuppression status at any age, and it is
computed for every campaign whether or not that campaign offers the cohort.

Combination rule - multiple evidence sources with OR logic.
KEY ELIGIBILITY GROUP for restricted 2025/26 campaigns.
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

-- Step 1: Find people with immunosuppression diagnosis (for all campaigns)
people_with_immuno_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_diagnosis_date,
        'Immunosuppression diagnosis' AS evidence_type
    FROM ({{ get_observations("'IMMDX_COV_COD'", 'UKHSA_COVID', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2: Find people with recent immunosuppression medications (for all campaigns)
people_with_recent_immuno_medications AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MAX(med.order_date) AS latest_medication_date,
        'Recent immunosuppression medication' AS evidence_type
    FROM ({{ get_medication_orders(cluster_id='IMMRX_COD', source='UKHSA_COVID', versioned=true) }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.spec_version = cc.terminology_version
        AND med.order_date IS NOT NULL
        AND med.order_date >= cc.recall_immuno_medication_lookback_date
        AND med.order_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, med.person_id
),

-- Step 3: Find people with recent immunosuppression administration codes (for all campaigns)
people_with_recent_immuno_admin AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_admin_date,
        'Recent immunosuppression administration' AS evidence_type
    FROM ({{ get_observations("'IMM_ADM_COD'", 'UKHSA_COVID', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.recall_immuno_admin_lookback_date
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 4: Find people with recent chemotherapy/radiotherapy (for all campaigns)
people_with_recent_chemo AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_chemo_date,
        'Recent chemotherapy/radiotherapy' AS evidence_type
    FROM ({{ get_observations("'DXT_CHEMO_COD'", 'UKHSA_COVID', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.recall_immuno_medication_lookback_date  -- same 6-month recall window
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 5: Union all evidence sources
all_immuno_evidence AS (
    SELECT 
        campaign_id, person_id, latest_diagnosis_date AS evidence_date, evidence_type
    FROM people_with_immuno_diagnosis
    
    UNION ALL
    
    SELECT 
        campaign_id, person_id, latest_medication_date AS evidence_date, evidence_type
    FROM people_with_recent_immuno_medications
    
    UNION ALL
    
    SELECT 
        campaign_id, person_id, latest_admin_date AS evidence_date, evidence_type
    FROM people_with_recent_immuno_admin
    
    UNION ALL
    
    SELECT 
        campaign_id, person_id, latest_chemo_date AS evidence_date, evidence_type
    FROM people_with_recent_chemo
),

-- Step 6: Get people with any immunosuppression evidence
people_with_immunosuppression AS (
    SELECT
        aie.campaign_id,
        aie.person_id,
        MAX(aie.evidence_date) AS latest_evidence_date,
        LISTAGG(DISTINCT aie.evidence_type, '; ') AS evidence_types,
        cc.campaign_reference_date,
        cc.audit_end_date
    FROM all_immuno_evidence aie
    LEFT JOIN all_campaigns cc ON aie.campaign_id = cc.campaign_id
    GROUP BY
        aie.campaign_id, aie.person_id, cc.campaign_reference_date, cc.audit_end_date
),

-- Step 7: Add age information and apply the 6-month floor (all campaigns)
people_immunosuppressed_with_age AS (
    SELECT
        pwi.campaign_id,
        pwi.person_id,
        demo.birth_date_approx,
        -- Completed years and months, not Snowflake's calendar-part subtraction, so the
        -- published age agrees with the age bounds tested on birth date downstream.
        FLOOR(MONTHS_BETWEEN(pwi.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        FLOOR(MONTHS_BETWEEN(pwi.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        pwi.latest_evidence_date,
        pwi.evidence_types,
        pwi.campaign_reference_date
    FROM people_with_immunosuppression pwi
    LEFT JOIN {{ ref('dim_person_demographics') }} demo
        ON pwi.person_id = demo.person_id
    WHERE demo.birth_date_approx IS NOT NULL
        -- Tested on birth date because Snowflake DATEDIFF subtracts calendar parts
        -- rather than counting completed months.
        AND demo.birth_date_approx <= DATEADD('month', -6, pwi.campaign_reference_date)
),

-- Step 8: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Immunosuppression' AS risk_group,
        person_id,
        latest_evidence_date AS qualifying_event_date,
        campaign_reference_date AS reference_date,
        CONCAT('Immunosuppressed patient: ', evidence_types) AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_immunosuppressed_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id