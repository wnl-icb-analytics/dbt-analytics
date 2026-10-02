{% macro nice_ind142(reference='current') %}
{#-
    Calculate NICE IND142 from dated LTC population and review evidence.
    Args: reference is current or by_month.
    Returns: the IND142 detail projection, one eligible person per reporting_date.
-#}
-- NICE IND142: https://www.nice.org.uk/indicators/ind142
-- Dementia care plan or review in 12 months, on or after diagnosis; the face-to-face setting is not coded.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        profile.earliest_dementia_diagnosis_date,
        review.latest_dementia_care_plan_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS review
        ON population.person_id = review.person_id
        AND population.reporting_date = review.reporting_date
    WHERE profile.has_dementia
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_dementia_care_plan_date AS latest_review_date,
        CASE
            WHEN population.latest_dementia_care_plan_date >= DATEADD(month, -12, population.reporting_date)
                THEN population.latest_dementia_care_plan_date
        END AS latest_record_date,
        COALESCE(population.latest_dementia_care_plan_date >= DATEADD(month, -12, population.reporting_date)
            AND population.latest_dementia_care_plan_date >= population.earliest_dementia_diagnosis_date, FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND142' AS indicator_id,
    'Dementia: care planning' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Dementia' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_review_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
