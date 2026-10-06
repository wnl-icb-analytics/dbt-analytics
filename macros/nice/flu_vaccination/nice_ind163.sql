{% macro nice_ind163(reference='current') %}
{#-
    Calculate NICE IND163 at each reference date with today's rules.
    Args: reference is current or by_month.
    Returns: indicator detail, one eligible person per reporting_date.
-#}
-- NICE IND163: https://www.nice.org.uk/indicators/ind163
-- Flu vaccination in the most recently completed season (1 August to 31 March) for people on the diabetes register; excludes a persisting contraindication or one recorded in 12 months.
WITH register AS (
    SELECT
        person_id,
        reporting_date
    FROM ({{ nice_register('DM', reference) }})
),

contraindications AS (
    SELECT
        person_id,
        MIN(IFF(is_persisting, clinical_effective_date::DATE, NULL)) AS first_persisting_date,
        MAX(IFF(NOT is_persisting, clinical_effective_date::DATE, NULL)) AS latest_expiring_date,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_flu_vaccination_contraindication_all') }}
    GROUP BY person_id, clinical_effective_date::DATE
),

contraindication_state AS (
    SELECT
        person_id,
        event_date,
        MIN(first_persisting_date) OVER (
            PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING
        ) AS first_persisting_date,
        MAX(latest_expiring_date) OVER (
            PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING
        ) AS latest_expiring_date
    FROM contraindications
),

indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        {{ nice_flu_season_year('population.reporting_date') }} AS season_year
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
    ASOF JOIN contraindication_state AS contra
        MATCH_CONDITION (population.reporting_date >= contra.event_date)
        ON population.person_id = contra.person_id
    WHERE contra.first_persisting_date IS NULL
        AND (contra.latest_expiring_date IS NULL
            OR contra.latest_expiring_date < DATEADD(month, -12, population.reporting_date))
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        season.latest_vaccination_date,
        season.is_laiv,
        season.latest_vaccination_date AS latest_record_date,
        season.person_id IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    -- March still selects the preceding completed season; the switch is on 1 April.
    LEFT JOIN {{ ref('int_nice_flu_season_vaccination_all') }} AS season
        ON population.person_id = season.person_id
        AND population.season_year = season.season_year
)

SELECT
    person_id,
    'IND163' AS indicator_id,
    'Immunisation: flu vaccine for people with diabetes' AS indicator_name,
    'The percentage of patients with diabetes, on the register, who have had influenza immunisation in the preceding 1 August to 31 March.' AS indicator_description,
    reporting_date AS reporting_date,
    DATE_FROM_PARTS({{ nice_flu_season_year('reporting_date') }}, 8, 1) AS measurement_period_start,
    age,
    'Diabetes, aged 17 or over' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_vaccination_date,
    is_laiv,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
