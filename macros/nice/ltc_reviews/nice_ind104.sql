{% macro nice_ind104(reference='current') %}
WITH reference_dates AS (
    SELECT reporting_date AS reference_date FROM ({{ nice_reference_dates(reference) }})
),
{% if reference == 'current' %}
new_depression AS (
    SELECT person_id, CURRENT_DATE()::DATE AS reporting_date,
        latest_new_depression_diagnosis_date AS diagnosis_date
    FROM {{ ref('int_ltc_review_profile') }}
)
{% else %}
keyed_diagnoses AS (
    SELECT person_id, clinical_effective_date::DATE AS diagnosis_date,
        TO_CHAR(clinical_effective_date, 'YYYYMMDDHH24MISS.FF9') || ':' || id::VARCHAR AS record_key,
        {{ ltc_known_date('clinical_effective_date', 'date_recorded') }} AS known_date
    FROM {{ ref('int_depression_diagnoses_all') }}
    WHERE is_diagnosis_code AND is_first_or_new_episode
),
selected_keys AS (
    {{ ltc_latest_known_record('SELECT person_id, record_key, MIN(known_date) AS known_date FROM keyed_diagnoses GROUP BY person_id, record_key', 'reference_dates') }}
),
diagnosis_payload AS (
    SELECT person_id, record_key, MIN(diagnosis_date) AS diagnosis_date
    FROM keyed_diagnoses GROUP BY person_id, record_key
),
new_depression AS (
    SELECT k.person_id, k.reference_date AS reporting_date, d.diagnosis_date
    FROM selected_keys k INNER JOIN diagnosis_payload d
        ON k.person_id = d.person_id AND k.record_key = d.record_key
)
{% endif %},
anchors AS (
    SELECT p.person_id, p.reporting_date, p.age, p.practice_code, p.practice_name, d.diagnosis_date
    FROM ({{ nice_reference_population(reference) }}) p
    INNER JOIN new_depression d ON p.person_id = d.person_id AND p.reporting_date = d.reporting_date
    WHERE p.age >= 18 AND d.diagnosis_date BETWEEN {{ nice_financial_year_start('p.reporting_date') }} AND p.reporting_date
),
follow_up AS (
    SELECT a.person_id, a.reporting_date, MIN(r.clinical_effective_date::DATE) AS latest_review_date
    FROM anchors a LEFT JOIN {{ ref('int_ltc_review_all') }} r
        ON a.person_id = r.person_id AND r.review_type = 'DEPRESSION_REVIEW'
        AND r.clinical_effective_date::DATE BETWEEN DATEADD(day, 10, a.diagnosis_date)
            AND LEAST(DATEADD(day, 35, a.diagnosis_date), a.reporting_date)
    GROUP BY a.person_id, a.reporting_date
)
SELECT a.person_id, 'IND104' AS indicator_id,
    'Depression and anxiety: review within 10 to 35 days' AS indicator_name,
    a.reporting_date, {{ nice_financial_year_start('a.reporting_date') }} AS measurement_period_start,
    a.age, 'New depression diagnosis since 1 April (aged 18 and over)' AS condition_name,
    a.practice_code AS {{ 'current_practice_code' if reference == 'current' else 'practice_code' }},
    a.practice_name AS {{ 'current_practice_name' if reference == 'current' else 'practice_name' }},
    a.diagnosis_date, f.latest_review_date, f.latest_review_date AS latest_record_date,
    TRUE AS is_in_denominator, f.latest_review_date IS NOT NULL AS is_in_numerator,
    IFF(f.latest_review_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM anchors a INNER JOIN follow_up f ON a.person_id = f.person_id AND a.reporting_date = f.reporting_date
{% endmacro %}
