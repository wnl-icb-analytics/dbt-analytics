{% macro nice_ind195(reference='current') %}
{#-
    Calculate NICE IND195 from dated LTC population and review evidence.
    Args: reference is current or by_month.
    Returns: the IND195 detail projection, one eligible person per reporting_date.
-#}
-- QOF HF007 permits separate assessment records.
-- NICE IND195: https://www.nice.org.uk/indicators/ind195
-- Heart failure review, NYHA assessment and medication review in 12 months for people on the heart failure register.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        review.latest_heart_failure_review_date,
        review.latest_nyha_date,
        review.latest_medication_review_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS review
        ON population.person_id = review.person_id
        AND population.reporting_date = review.reporting_date
    WHERE profile.has_heart_failure
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_heart_failure_review_date AS latest_review_date,
        population.latest_nyha_date AS latest_nyha_date,
        population.latest_medication_review_date AS latest_medication_review_date,
        CASE
            WHEN population.latest_heart_failure_review_date >= DATEADD(month, -12, population.reporting_date)
                THEN population.latest_heart_failure_review_date
        END AS latest_record_date,
        COALESCE(population.latest_heart_failure_review_date >= DATEADD(month, -12, population.reporting_date)
            AND population.latest_nyha_date >= DATEADD(month, -12, population.reporting_date)
            AND population.latest_medication_review_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND195' AS indicator_id,
    'Heart failure: annual review' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Heart failure' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_review_date,
    latest_nyha_date,
    latest_medication_review_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
