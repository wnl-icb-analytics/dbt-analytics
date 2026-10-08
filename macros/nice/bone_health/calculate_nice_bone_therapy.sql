{#-
    Select bone-sparing order and hospital therapy evidence for the NICE bone health population.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date with order and hospital therapy dates.
-#}
{% macro calculate_nice_bone_therapy(reference='current') %}
WITH population AS (
    SELECT person_id, reporting_date
    FROM ({{ nice_reference_population(reference) }})
    WHERE age >= 50
), daily AS (
    SELECT person_id, order_date::DATE AS order_date
    FROM {{ ref('int_bone_sparing_medications_all') }}
    GROUP BY person_id, order_date::DATE
), hospital_daily AS (
    SELECT person_id, therapy_date, coverage_months
    FROM {{ ref('int_bone_sparing_hospital_therapy_all') }}
    GROUP BY person_id, therapy_date, coverage_months
), hospital AS (
    SELECT p.person_id, p.reporting_date,
        MAX(h.therapy_date) AS latest_hospital_therapy_date,
        -- An older annual treatment can still cover R after a newer six-month treatment expires.
        MAX(IFF(h.therapy_date > DATEADD(month, -h.coverage_months, p.reporting_date),
            h.therapy_date, NULL)) AS latest_current_hospital_therapy_date
    FROM population p
    INNER JOIN hospital_daily h ON p.person_id = h.person_id AND h.therapy_date <= p.reporting_date
    GROUP BY p.person_id, p.reporting_date
)
SELECT population.person_id, population.reporting_date,
    daily.order_date AS latest_bone_sparing_order_date,
    hospital.latest_hospital_therapy_date, hospital.latest_current_hospital_therapy_date
FROM population
ASOF JOIN daily
    MATCH_CONDITION (population.reporting_date >= daily.order_date)
    ON population.person_id = daily.person_id
LEFT JOIN hospital ON population.person_id = hospital.person_id
    AND population.reporting_date = hospital.reporting_date
{% endmacro %}
