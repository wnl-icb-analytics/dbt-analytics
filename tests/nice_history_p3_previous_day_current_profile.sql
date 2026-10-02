{{ config(tags=['monthly-full', 'nice-history']) }}

{% set query = namespace(sql=nice_ind127('current')) %}
{% set query.sql = query.sql | replace(nice_reference_population('current') | string, 'SELECT * FROM synthetic_population') %}
{% set query.sql = query.sql | replace(ref('int_atrial_fibrillation_profile') | string, 'synthetic_profile') %}

WITH synthetic_population AS (
    SELECT
        people.column1::NUMBER AS person_id,
        CURRENT_DATE()::DATE AS reporting_date,
        50 AS age,
        'SYNTHETIC'::VARCHAR AS practice_code,
        'Synthetic practice'::VARCHAR AS practice_name
    FROM VALUES (1) AS people
    CROSS JOIN (SELECT column1 FROM VALUES ('2024-01-31')) AS dates
),
synthetic_profile AS (
    SELECT
        population.person_id AS person_id,
        NULL::DATE AS earliest_af_diagnosis_date,
        3::FLOAT AS latest_chadsvasc_score,
        CURRENT_DATE()::DATE AS latest_chadsvasc_date,
        NULL::FLOAT AS latest_chads2_score,
        NULL::DATE AS latest_chads2_date,
        CURRENT_DATE()::DATE AS latest_stroke_risk_score_date,
        3::FLOAT AS latest_stroke_risk_score,
        NULL::FLOAT AS max_stroke_risk_score_ever,
        NULL::FLOAT AS max_stroke_risk_score_before_period,
        NULL::DATE AS latest_anticoagulant_order_date,
        NULL::VARCHAR AS latest_anticoagulant_type,
        NULL::DATE AS latest_doac_order_date,
        NULL::DATE AS latest_vka_order_date,
        FALSE AS has_anticoagulant_adverse_reaction,
        FALSE AS has_anticoagulant_persisting_contraindication,
        NULL::DATE AS latest_anticoagulant_contraindicated_date,
        NULL::DATE AS latest_anticoagulant_declined_date,
        FALSE AS is_doac_ineligible,
        FALSE AS has_doac_exception,
        NULL::DATE AS latest_anticoagulant_review_date,
        DATEADD(day, -1, population.reporting_date)::DATE AS reporting_date
    FROM synthetic_population AS population
),
actual AS ({{ query.sql }})
SELECT COUNT(*) AS failure_count
FROM actual
HAVING COUNT(*) <> 1
    OR COALESCE(COUNT_IF(reporting_date = CURRENT_DATE() AND is_in_numerator), 0) <> 1
