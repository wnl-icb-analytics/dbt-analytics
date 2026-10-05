{% macro nice_ind78(reference='current') %}
{#-
    Calculate NICE IND78 for eligible women at each reference date.
    Args: reference is current or by_month.
    Returns: the IND78 detail columns, one person per reporting_date.
-#}
-- NICE IND78: https://www.nice.org.uk/indicators/ind78
WITH population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        evidence.latest_asm_date,
        register.person_id IS NOT NULL AS has_epilepsy
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_contraception_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    LEFT JOIN ({{ nice_register('EP', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
    WHERE population.gender = 'Female'
        AND population.age < 55
        AND evidence.latest_asm_date > DATEADD(month, -6, population.reporting_date)
        AND evidence.latest_asm_date <= population.reporting_date
        AND (evidence.first_sterilisation_hysterectomy_date IS NULL
            OR evidence.first_sterilisation_hysterectomy_date > population.reporting_date)
), contraception_daily AS (
    SELECT person_id, event_date, id
    FROM {{ ref('int_nice_reproductive_advice_topics_all') }}
    WHERE is_contraception_topic
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, event_date ORDER BY id DESC) = 1
), pregnancy_daily AS (
    SELECT person_id, event_date, id
    FROM {{ ref('int_nice_reproductive_advice_topics_all') }}
    WHERE is_pregnancy_topic
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, event_date ORDER BY id DESC) = 1
), assessed AS (
    SELECT population.*,
        IFF(contraception.event_date >= DATEADD(month, -12, population.reporting_date),
            contraception.event_date, NULL) AS latest_contraception_advice_date,
        IFF(pregnancy.event_date >= DATEADD(month, -12, population.reporting_date),
            pregnancy.event_date, NULL) AS latest_pregnancy_advice_date,
        latest_contraception_advice_date IS NOT NULL AND latest_pregnancy_advice_date IS NOT NULL AS has_both_topics,
        IFF(has_both_topics, GREATEST(latest_contraception_advice_date, latest_pregnancy_advice_date), NULL) AS latest_record_date
    FROM population
    ASOF JOIN contraception_daily AS contraception
        MATCH_CONDITION (population.reporting_date >= contraception.event_date)
        ON population.person_id = contraception.person_id
    ASOF JOIN pregnancy_daily AS pregnancy
        MATCH_CONDITION (population.reporting_date >= pregnancy.event_date)
        ON population.person_id = pregnancy.person_id
)
SELECT person_id, 'IND78' AS indicator_id,
    'Contraception: advice for people taking anti-seizure medication' AS indicator_name,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Women under 55 taking antiseizure medication' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    latest_asm_date,
    has_epilepsy,
    latest_contraception_advice_date, latest_pregnancy_advice_date,
    latest_record_date, TRUE AS is_in_denominator,
    has_both_topics AS is_in_numerator,
    IFF(has_both_topics, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed AS result
{% endmacro %}
