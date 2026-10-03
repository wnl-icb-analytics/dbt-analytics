{#-
    Calculate NICE IND92 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND92 detail columns, one person per reporting_date.
-#}
{% macro nice_ind92(reference='current') %}
-- NICE IND92: https://www.nice.org.uk/indicators/ind92
-- Bone-sparing medication ordered in six months for people aged 75 or over with a fragility fracture since 1 April 2012.
WITH indicator_population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN (
        SELECT person_id, MIN(clinical_effective_date::DATE) AS first_fracture_date
        FROM {{ ref('int_fragility_fractures_all') }}
        WHERE clinical_effective_date::DATE >= '2012-04-01'::DATE
        GROUP BY person_id
    ) AS fracture
        ON population.person_id = fracture.person_id
        AND fracture.first_fracture_date <= population.reporting_date
    WHERE population.age >= 75
), assessed AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        therapy.latest_bone_sparing_order_date,
        COALESCE(therapy.latest_bone_sparing_order_date > DATEADD(month, -6, population.reporting_date)
            AND therapy.latest_bone_sparing_order_date <= population.reporting_date, FALSE) AS is_in_numerator
    FROM indicator_population AS population
    LEFT JOIN {{ nice_ref('int_nice_bone_therapy', reference) }} AS therapy
        ON population.person_id = therapy.person_id
        AND population.reporting_date = therapy.reporting_date
)
SELECT person_id, 'IND92' AS indicator_id,
    'Osteoporosis: bone-sparing agents (75 years and over)' AS indicator_name,
    reporting_date, DATEADD(month, -6, reporting_date) AS measurement_period_start,
    age, 'Fragility fracture' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_bone_sparing_order_date,
    IFF(is_in_numerator, latest_bone_sparing_order_date, NULL)::DATE AS latest_record_date,
    TRUE AS is_in_denominator, is_in_numerator,
    CASE WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_bone_sparing_order_date IS NULL THEN 'NEVER_TREATED'
        ELSE 'NOT_TREATED_IN_PERIOD' END AS indicator_status
FROM assessed
{% endmacro %}
