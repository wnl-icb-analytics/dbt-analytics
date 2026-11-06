{{ config(tags=['monthly-full', 'nice-history']) }}

{% set profile = namespace(sql=calculate_nice_atrial_fibrillation_profile('by_month')) %}
{% set profile.sql = profile.sql | replace(nice_register('AF', 'by_month') | string, 'SELECT * FROM synthetic_register') %}
{% for model, fixture in [
    ('int_anticoagulant_medications_all', 'synthetic_orders'),
    ('int_stroke_risk_score_all', 'synthetic_scores'),
    ('int_ttr_all', 'synthetic_ttr'),
    ('int_anticoagulant_exception_all', 'synthetic_exceptions'),
    ('int_anticoagulant_review_all', 'synthetic_reviews')
] %}
    {% set profile.sql = profile.sql | replace(ref(model) | string, fixture) %}
{% endfor %}
{% set indicator = namespace(sql=nice_ind247('by_month')) %}
{% set indicator.sql = indicator.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set indicator.sql = indicator.sql | replace(nice_ref('int_atrial_fibrillation_profile', 'by_month') | string, 'synthetic_profile') %}

WITH synthetic_population AS (
    SELECT people.column1::NUMBER AS person_id, dates.column1::DATE AS reporting_date,
        70 AS age, 'SYNTHETIC'::VARCHAR AS practice_code, 'Synthetic practice'::VARCHAR AS practice_name
    FROM VALUES (-143301), (-143302) AS people
    CROSS JOIN (SELECT column1 FROM VALUES ('2024-07-31'), ('2024-08-01')) AS dates
),
synthetic_register AS (
    SELECT person_id, reporting_date, '2023-01-01'::DATE AS earliest_diagnosis_date
    FROM synthetic_population
),
synthetic_orders AS (
    -- Each person has only a retained, unlisted order. Class flags still carry chemical evidence.
    SELECT column1::NUMBER AS person_id, column2::VARCHAR AS medication_order_id,
        '2024-01-31'::DATE AS order_date, column3::VARCHAR AS anticoagulant_type,
        anticoagulant_type = 'DOAC' AS is_doac, anticoagulant_type = 'VKA' AS is_vka,
        FALSE AS is_in_national_anticoagulant_list, NULL::VARCHAR AS anticoagulant_type_national
    FROM VALUES (-143301, 'SYN_RETAINED_DOAC', 'DOAC'), (-143302, 'SYN_RETAINED_VKA', 'VKA')
),
synthetic_scores AS (
    SELECT DISTINCT person_id, 'SYN_SCORE'::VARCHAR AS id, '2024-01-01'::DATE AS clinical_effective_date,
        'CHA2DS2-VASc'::VARCHAR AS score_type, 3::FLOAT AS score_value
    FROM synthetic_population
),
synthetic_ttr AS (
    SELECT NULL::NUMBER AS person_id, NULL::VARCHAR AS id, NULL::DATE AS clinical_effective_date,
        NULL::FLOAT AS original_result_value, NULL::FLOAT AS ttr_percentage WHERE FALSE
),
synthetic_exceptions AS (
    SELECT -143302::NUMBER AS person_id, '2023-01-01'::DATE AS clinical_effective_date,
        'VALVULAR_AF'::VARCHAR AS exception_type
),
synthetic_reviews AS (
    SELECT NULL::NUMBER AS person_id, NULL::VARCHAR AS id, NULL::DATE AS clinical_effective_date WHERE FALSE
),
synthetic_profile AS ({{ profile.sql }}),
actual AS ({{ indicator.sql }}),
failures AS (
    SELECT person_id, reporting_date
    FROM actual
    WHERE indicator_status IS DISTINCT FROM IFF(reporting_date = '2024-07-31', 'ACHIEVED', 'NOT_TREATED_IN_PERIOD')
        OR latest_anticoagulant_type IS DISTINCT FROM IFF(person_id = -143301, 'DOAC', 'VKA')
        OR (person_id = -143301 AND latest_doac_order_date IS DISTINCT FROM '2024-01-31'::DATE)
        OR (person_id = -143302 AND latest_vka_order_date IS DISTINCT FROM '2024-01-31'::DATE)
)
SELECT (SELECT COUNT(*) FROM failures) + ABS((SELECT COUNT(*) FROM actual) - 4) AS failure_count
HAVING failure_count > 0
