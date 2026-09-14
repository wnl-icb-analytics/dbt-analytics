/*
Asthma Eligibility Rule

Business Rule: A person aged 6 months or over is eligible when either:
1. They have an emergency asthma admission, or
2. They have an asthma diagnosis and recent asthma medication evidence.

An emergency admission qualifies without a separate diagnosis or medication entry.
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

asthma_diagnosis AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_asthma_date
    FROM ({{ get_observations("'AST_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

recent_asthma_prescriptions AS (
    SELECT
        cc.campaign_id,
        med.person_id,
        MAX(med.order_date) AS medication_date
    FROM ({{ get_medication_orders(cluster_id='ASTRX_COD', source='UKHSA_FLU', versioned=true) }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.spec_version = cc.terminology_version
        AND med.order_date >= cc.asthma_medication_lookback_date
        AND med.order_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, med.person_id
),

recent_asthma_administration AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS medication_date
    FROM ({{ get_observations("'ASTMED_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date >= cc.asthma_medication_lookback_date
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

recent_asthma_medication AS (
    SELECT
        campaign_id,
        person_id,
        MAX(medication_date) AS medication_date
    FROM (
        SELECT campaign_id, person_id, medication_date FROM recent_asthma_prescriptions
        UNION ALL
        SELECT campaign_id, person_id, medication_date FROM recent_asthma_administration
    )
    GROUP BY campaign_id, person_id
),

active_asthma AS (
    SELECT
        diagnosis.campaign_id,
        diagnosis.person_id,
        medication.medication_date AS qualifying_event_date
    FROM asthma_diagnosis diagnosis
    JOIN recent_asthma_medication medication
        ON diagnosis.campaign_id = medication.campaign_id
        AND diagnosis.person_id = medication.person_id
),

asthma_admission AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS qualifying_event_date
    FROM ({{ get_observations("'ASTADM_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

eligible_asthma AS (
    SELECT
        campaign_id,
        person_id,
        MAX(qualifying_event_date) AS qualifying_event_date
    FROM (
        SELECT campaign_id, person_id, qualifying_event_date FROM active_asthma
        UNION ALL
        SELECT campaign_id, person_id, qualifying_event_date FROM asthma_admission
    )
    GROUP BY campaign_id, person_id
),

final_eligibility AS (
    SELECT
        asthma.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Asthma' AS risk_group,
        asthma.person_id,
        asthma.qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'People with a qualifying asthma admission or actively managed asthma' AS description,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM eligible_asthma asthma
    JOIN all_campaigns cc
        ON asthma.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON asthma.person_id = demo.person_id
    WHERE DATEADD('month', 6, demo.birth_date_approx) <= cc.run_date
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id
