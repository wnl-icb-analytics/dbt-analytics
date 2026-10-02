{% macro nice_ind273(reference='current') %}
WITH population AS ({{ nice_asthma_diagnosis_population(reference) }}),
{% if reference == 'current' %}
evidence AS (
    SELECT person_id, CURRENT_DATE()::DATE AS reporting_date,
        latest_asthma_review_date AS latest_review_date,
        latest_complete_asthma_review_date
    FROM {{ ref('int_ltc_review_profile') }}
)
{% else %}
review_dates AS (
    SELECT person_id, review_date FROM {{ ref('int_nice_asthma_review_all') }} WHERE has_review
),
complete_dates AS (
    SELECT person_id, review_date FROM {{ ref('int_nice_asthma_review_all') }} WHERE is_complete_review
),
evidence AS (
    SELECT p.person_id, p.reporting_date, review.review_date AS latest_review_date,
        complete.review_date AS latest_complete_asthma_review_date
    FROM population p
    ASOF JOIN review_dates review MATCH_CONDITION (p.reporting_date >= review.review_date)
        ON p.person_id = review.person_id
    ASOF JOIN complete_dates complete MATCH_CONDITION (p.reporting_date >= complete.review_date)
        ON p.person_id = complete.person_id
)
{% endif %},
assessed AS (
    SELECT p.person_id, p.reporting_date, p.age, p.practice_code, p.practice_name,
        e.latest_review_date,
        CASE WHEN e.latest_complete_asthma_review_date >= DATEADD(month, -12, p.reporting_date)
            THEN e.latest_complete_asthma_review_date END AS latest_record_date,
        COALESCE(e.latest_complete_asthma_review_date >= DATEADD(month, -12, p.reporting_date), FALSE) AS is_in_numerator
    FROM population p INNER JOIN evidence e
        ON p.person_id = e.person_id AND p.reporting_date = e.reporting_date
)
SELECT person_id, 'IND273' AS indicator_id, 'Asthma: annual review' AS indicator_name,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Asthma (aged 5 and over)' AS condition_name,
    practice_code AS {{ 'current_practice_code' if reference == 'current' else 'practice_code' }},
    practice_name AS {{ 'current_practice_name' if reference == 'current' else 'practice_name' }},
    latest_review_date, latest_record_date, TRUE AS is_in_denominator, is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
