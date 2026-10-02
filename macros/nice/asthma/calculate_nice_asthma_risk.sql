{% macro calculate_nice_asthma_risk(reference='current') %}
WITH population AS (
    SELECT person_id, reporting_date
    FROM ({{ nice_asthma_diagnosis_population(reference) }})
    WHERE age >= 6
), medication_counts AS (
    SELECT p.person_id, p.reporting_date,
        COALESCE(SUM(e.saba_inhaler_count), 0) AS saba_inhaler_count,
        COUNT(DISTINCT IFF(e.is_prednisolone, e.order_date, NULL)) AS oral_steroid_course_count
    FROM population p
    LEFT JOIN {{ ref('int_nice_asthma_risk_medications_all') }} e
        ON p.person_id = e.person_id
        AND e.order_date > DATEADD(month, -24, p.reporting_date)
        AND e.order_date <= DATEADD(month, -12, p.reporting_date)
    GROUP BY p.person_id, p.reporting_date
), admissions AS (
    SELECT p.person_id, p.reporting_date, MAX(e.event_date) AS latest_asthma_admission_date
    FROM population p
    LEFT JOIN {{ ref('int_nice_asthma_admissions_all') }} e
        ON p.person_id = e.person_id
        AND e.event_date > DATEADD(month, -24, p.reporting_date)
        AND e.event_date <= DATEADD(month, -12, p.reporting_date)
    GROUP BY p.person_id, p.reporting_date
)
SELECT p.person_id, p.reporting_date,
    DATEADD(month, -24, p.reporting_date) AS risk_period_start,
    DATEADD(month, -12, p.reporting_date) AS risk_period_end,
    m.saba_inhaler_count, m.oral_steroid_course_count, a.latest_asthma_admission_date,
    m.saba_inhaler_count >= 6 AS has_high_saba_use,
    m.oral_steroid_course_count >= 2 AS has_repeated_oral_steroids,
    a.latest_asthma_admission_date IS NOT NULL AS has_asthma_admission,
    m.saba_inhaler_count >= 6 OR m.oral_steroid_course_count >= 2
        OR a.latest_asthma_admission_date IS NOT NULL AS is_higher_risk
FROM population p
INNER JOIN medication_counts m ON p.person_id = m.person_id AND p.reporting_date = m.reporting_date
INNER JOIN admissions a ON p.person_id = a.person_id AND p.reporting_date = a.reporting_date
{% endmacro %}
