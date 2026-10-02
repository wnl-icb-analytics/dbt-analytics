{% macro nice_ind273(reference='current') %}
{#-
    Calculate NICE IND273 from unresolved diagnoses and complete review events.
    Args: reference is current or by_month.
    Returns: the IND273 detail projection, one eligible person per reporting_date.
-#}
-- NICE IND273: https://www.nice.org.uk/indicators/ind273
-- Asthma review in 12 months with a same-day written plan and an exacerbation count in the preceding month, for people aged 5 and over.
WITH population AS (
    {{ nice_asthma_diagnosis_population(reference) }}
),

review_dates AS (
    SELECT
        person_id,
        review_date
    FROM {{ ref('int_nice_asthma_review_all') }}
    WHERE has_review
),

complete_dates AS (
    SELECT
        person_id,
        review_date
    FROM {{ ref('int_nice_asthma_review_all') }}
    WHERE is_complete_review
),

evidence AS (
    SELECT
        population.person_id,
        population.reporting_date,
        review.review_date AS latest_review_date,
        complete.review_date AS latest_complete_asthma_review_date
    FROM population
    ASOF JOIN review_dates AS review
        MATCH_CONDITION (population.reporting_date >= review.review_date)
        ON population.person_id = review.person_id
    -- A later incomplete review does not displace an earlier complete review.
    ASOF JOIN complete_dates AS complete
        MATCH_CONDITION (population.reporting_date >= complete.review_date)
        ON population.person_id = complete.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        evidence.latest_review_date,
        CASE
            WHEN evidence.latest_complete_asthma_review_date >= DATEADD(month, -12, population.reporting_date)
                THEN evidence.latest_complete_asthma_review_date
        END AS latest_record_date,
        COALESCE(evidence.latest_complete_asthma_review_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM population
    INNER JOIN evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
)

SELECT
    assessed.person_id,
    'IND273' AS indicator_id,
    'Asthma: annual review' AS indicator_name,
    assessed.reporting_date,
    DATEADD(month, -12, assessed.reporting_date) AS measurement_period_start,
    assessed.age,
    'Asthma (aged 5 and over)' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    assessed.latest_review_date,
    assessed.latest_record_date,
    TRUE AS is_in_denominator,
    assessed.is_in_numerator,
    IFF(assessed.is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
