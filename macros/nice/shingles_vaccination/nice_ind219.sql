{% macro nice_ind219(reference='current') %}
{#-
    Calculate NICE IND219 at each reference date with today's rules.
    Args: reference is current or by_month.
    Returns: indicator detail, one eligible person per reporting_date.
-#}
-- NICE IND219: https://www.nice.org.uk/indicators/ind219
-- Shingles vaccination between the 70th and 75th birthdays for people who reached 75 in the preceding 12 months; excludes immunosuppressed people.
WITH immunosuppression AS (
    {{ calculate_nice_immunosuppression(reference) }}
),

indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.birth_date_approx::DATE AS birth_date_approx,
        DATEADD(year, 75, population.birth_date_approx)::DATE AS seventy_fifth_birthday
    FROM ({{ nice_reference_population(reference) }}) AS population
    LEFT JOIN immunosuppression
        ON population.person_id = immunosuppression.person_id
        AND population.reporting_date = immunosuppression.reporting_date
    WHERE DATEADD(year, 75, population.birth_date_approx) > DATEADD(month, -12, population.reporting_date)
        AND DATEADD(year, 75, population.birth_date_approx) <= population.reporting_date
        -- Reconstruct the current COVID proxy from events, including pre-campaign history.
        AND immunosuppression.person_id IS NULL
),

doses AS (
    SELECT
        population.person_id,
        population.reporting_date,
        MIN(shingles.clinical_effective_date::DATE) AS first_dose_70_to_75_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_shingles_vaccination_all') }} AS shingles
        ON population.person_id = shingles.person_id
        AND shingles.is_administered
        AND shingles.clinical_effective_date::DATE
            BETWEEN DATEADD(year, 70, population.birth_date_approx) AND population.seventy_fifth_birthday
        AND shingles.clinical_effective_date::DATE <= population.reporting_date
    GROUP BY population.person_id, population.reporting_date
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.seventy_fifth_birthday,
        doses.first_dose_70_to_75_date,
        doses.first_dose_70_to_75_date AS latest_record_date,
        doses.person_id IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    LEFT JOIN doses
        ON population.person_id = doses.person_id
        AND population.reporting_date = doses.reporting_date
)

SELECT
    person_id,
    'IND219' AS indicator_id,
    'Immunisation: shingles' AS indicator_name,
    'The percentage of patients who reached 75 years old in the preceding 12 months, who have received a shingles vaccine between the ages of 70 and 75 years.' AS indicator_description,
    reporting_date AS reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'People reaching 75 in the preceding 12 months' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    seventy_fifth_birthday,
    first_dose_70_to_75_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
