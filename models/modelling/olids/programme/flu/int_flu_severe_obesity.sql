/*
Severe Obesity Eligibility Rule

Business Rule: A person aged 18 or over at the campaign audit end is eligible when either:
1. Their latest BMI is at least 40 and is not older than their latest BMI stage, or
2. Their latest BMI stage is severe obesity and is later than their latest BMI.

SEV_OBESITY_COD is a subset of BMI_STAGE_COD. A severe obesity code therefore
qualifies only when it is also the latest BMI stage entry.
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

latest_bmi AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        obs.clinical_effective_date AS bmi_date,
        TRY_CAST(obs.result_value AS FLOAT) AS bmi_value
    FROM ({{ get_observations("'BMI_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        AND TRY_CAST(obs.result_value AS FLOAT) > 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cc.campaign_id, obs.person_id
        ORDER BY obs.clinical_effective_date DESC, obs.date_recorded DESC, obs.id DESC
    ) = 1
),

latest_bmi_stage AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS bmi_stage_date
    FROM ({{ get_observations("'BMI_STAGE_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

latest_severe_obesity AS (
    SELECT
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS severe_obesity_date
    FROM ({{ get_observations("'SEV_OBESITY_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

people_with_obesity_evidence AS (
    SELECT campaign_id, person_id FROM latest_bmi
    UNION
    SELECT campaign_id, person_id FROM latest_bmi_stage
),

evaluated_obesity_evidence AS (
    SELECT
        people.campaign_id,
        people.person_id,
        CASE
            WHEN severe.severe_obesity_date = stage.bmi_stage_date
                AND (bmi.bmi_date IS NULL OR severe.severe_obesity_date > bmi.bmi_date)
                THEN severe.severe_obesity_date
            WHEN bmi.bmi_value >= 40
                AND (stage.bmi_stage_date IS NULL OR bmi.bmi_date >= stage.bmi_stage_date)
                THEN bmi.bmi_date
        END AS qualifying_event_date
    FROM people_with_obesity_evidence people
    LEFT JOIN latest_bmi bmi
        ON people.campaign_id = bmi.campaign_id
        AND people.person_id = bmi.person_id
    LEFT JOIN latest_bmi_stage stage
        ON people.campaign_id = stage.campaign_id
        AND people.person_id = stage.person_id
    LEFT JOIN latest_severe_obesity severe
        ON people.campaign_id = severe.campaign_id
        AND people.person_id = severe.person_id
),

eligible_obesity_evidence AS (
    SELECT campaign_id, person_id, qualifying_event_date
    FROM evaluated_obesity_evidence
    WHERE qualifying_event_date IS NOT NULL
),

final_eligibility AS (
    SELECT
        evidence.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Morbid Obesity' AS risk_group,
        evidence.person_id,
        evidence.qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'Adults aged 18 or over with severe obesity (BMI 40 or above)' AS description,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM eligible_obesity_evidence evidence
    JOIN all_campaigns cc
        ON evidence.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON evidence.person_id = demo.person_id
    WHERE DATEADD('year', 18, demo.birth_date_approx) <= cc.audit_end_date
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id
