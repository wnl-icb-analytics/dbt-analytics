{% macro nice_ind164(reference='current') %}
{#-
    Calculate NICE IND164 at each reference date with today's rules.
    Args: reference is current or by_month.
    Returns: indicator detail, one eligible person per reporting_date.
-#}
-- NICE IND164: https://www.nice.org.uk/indicators/ind164
-- Flu vaccination in the most recently completed season (1 August to 31 March) for people on the stroke/TIA register, before personalised care adjustments.
WITH register AS (
    SELECT
        person_id,
        reporting_date
    FROM ({{ nice_register('STIA', reference) }})
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
    'IND164' AS indicator_id,
    'Immunisation: flu vaccine for people with stroke or TIA' AS indicator_name,
    'The percentage of patients with stroke or TIA who have had influenza immunisation in the preceding 1 August to 31 March.' AS indicator_description,
    reporting_date AS reporting_date,
    DATE_FROM_PARTS({{ nice_flu_season_year('reporting_date') }}, 8, 1) AS measurement_period_start,
    age,
    'Stroke or transient ischaemic attack' AS denominator_description,
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
