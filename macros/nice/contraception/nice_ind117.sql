{% macro nice_ind117(reference='current') %}
WITH population AS (
    SELECT population.*
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('EP', reference) }}) AS register
        ON population.person_id = register.person_id AND population.reporting_date = register.reporting_date
    WHERE population.gender = 'Female' AND population.age >= 18 AND population.age < 45
        AND NOT EXISTS (
            SELECT 1 FROM {{ ref('int_nice_sterilisation_hysterectomy_all') }} AS exclusion
            WHERE exclusion.person_id = population.person_id AND exclusion.event_date <= population.reporting_date
        )
), daily_advice AS (
    SELECT person_id, event_date, id
    FROM {{ ref('int_nice_epilepsy_pregnancy_advice_all') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, event_date ORDER BY id DESC, source_cluster_id DESC) = 1
), assessed AS (
    SELECT population.*,
        IFF(advice.event_date >= DATEADD(month, -12, population.reporting_date), advice.event_date, NULL) AS latest_record_date
    FROM population
    ASOF JOIN daily_advice AS advice
        MATCH_CONDITION (population.reporting_date >= advice.event_date)
        ON population.person_id = advice.person_id
)
SELECT person_id, 'IND117' AS indicator_id,
    'Contraception: advice for women with epilepsy' AS indicator_name,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Women aged 18 to 44 on the epilepsy register' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    latest_record_date, TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed AS result
{% endmacro %}
