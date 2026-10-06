{% macro nice_ind178(reference='current') %}
{#-
    Calculate NICE IND178 for women after their latest delivery episode.
    Args: reference is current or by_month.
    Returns: the IND178 detail columns, one person per reporting_date.
-#}
-- NICE IND178: https://www.nice.org.uk/indicators/ind178
WITH RECURSIVE population AS (
    SELECT person_id, reporting_date, age, birth_date_approx, practice_code, practice_name
    FROM ({{ nice_reference_population(reference) }})
    WHERE gender = 'Female'
),
delivery_days AS (
    SELECT DISTINCT population.person_id, population.reporting_date,
        event.clinical_effective_date::DATE AS clinical_date
    FROM population
    INNER JOIN {{ ref('int_maternal_deliveries_all') }} AS event
        ON population.person_id = event.person_id
        AND event.clinical_effective_date::DATE <= population.reporting_date
        AND (event.date_recorded::DATE <= population.reporting_date OR event.date_recorded IS NULL)
    WHERE FLOOR(DATEDIFF(month, population.birth_date_approx, event.clinical_effective_date::DATE) / 12)
        BETWEEN 12 AND 55
),
ordered_days AS (
    SELECT person_id, reporting_date, clinical_date,
        ROW_NUMBER() OVER (PARTITION BY person_id, reporting_date ORDER BY clinical_date) AS event_number
    FROM delivery_days
),
episodes (person_id, reporting_date, event_number, delivery_date) AS (
    SELECT person_id, reporting_date, event_number, clinical_date
    FROM ordered_days
    WHERE event_number = 1

    UNION ALL

    SELECT next_day.person_id, next_day.reporting_date, next_day.event_number,
        -- Measure spacing from the episode's first record, not the previous record.
        IFF(next_day.clinical_date > DATEADD(day, 180, episodes.delivery_date),
            next_day.clinical_date, episodes.delivery_date)
    FROM episodes
    INNER JOIN ordered_days AS next_day
        ON episodes.person_id = next_day.person_id
        AND episodes.reporting_date = next_day.reporting_date
        AND next_day.event_number = episodes.event_number + 1
),
latest_delivery AS (
    SELECT person_id, reporting_date, delivery_date
    FROM episodes
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, reporting_date ORDER BY event_number DESC) = 1
),
indicator_population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name, delivery.delivery_date
    FROM population
    INNER JOIN latest_delivery AS delivery
        ON population.person_id = delivery.person_id
        AND population.reporting_date = delivery.reporting_date
    WHERE delivery.delivery_date > DATEADD(month, -12, population.reporting_date)
        AND delivery.delivery_date <= DATEADD(week, -16, population.reporting_date)
),
screening AS (
    SELECT population.person_id, population.reporting_date,
        MAX(event.clinical_effective_date::DATE) AS latest_record_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_mental_health_screening_all') }} AS event
        ON population.person_id = event.person_id
        AND event.clinical_effective_date::DATE BETWEEN DATEADD(week, 4, population.delivery_date)
            AND LEAST(DATEADD(week, 16, population.delivery_date), population.reporting_date)
    GROUP BY population.person_id, population.reporting_date
)
SELECT population.person_id, 'IND178' AS indicator_id,
    'Pregnancy and neonates: postnatal mental health' AS indicator_name,
    population.reporting_date, DATEADD(month, -12, population.reporting_date) AS measurement_period_start,
    population.age, 'Women after their latest delivery episode' AS condition_name,
    {{ nice_practice_columns('population', reference) }},
    population.delivery_date, screening.latest_record_date,
    TRUE AS is_in_denominator, screening.latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(screening.latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM indicator_population AS population
LEFT JOIN screening
    ON population.person_id = screening.person_id
    AND population.reporting_date = screening.reporting_date
{% endmacro %}
