{% macro nice_ind191(reference='current') %}
{#-
    Calculate NICE IND191 from dated LTC population and review evidence.
    Args: reference is current or by_month.
    Returns: the IND191 detail projection, one eligible person per reporting_date.
-#}
-- QOF COPD010 permits separate assessment records.
-- NICE IND191: https://www.nice.org.uk/indicators/ind191
-- COPD review, exacerbation count and MRC dyspnoea assessment in 12 months for people on the COPD register.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        review.latest_copd_review_date,
        review.latest_mrc_dyspnoea_date,
        review.latest_copd_exacerbation_count_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS review
        ON population.person_id = review.person_id
        AND population.reporting_date = review.reporting_date
    WHERE profile.has_copd
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_copd_review_date AS latest_review_date,
        population.latest_mrc_dyspnoea_date AS latest_mrc_dyspnoea_date,
        population.latest_copd_exacerbation_count_date,
        CASE
            WHEN population.latest_copd_review_date >= DATEADD(month, -12, population.reporting_date)
                THEN population.latest_copd_review_date
        END AS latest_record_date,
        COALESCE(population.latest_copd_review_date >= DATEADD(month, -12, population.reporting_date)
            AND population.latest_mrc_dyspnoea_date >= DATEADD(month, -12, population.reporting_date)
            AND population.latest_copd_exacerbation_count_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND191' AS indicator_id,
    'COPD: annual review' AS indicator_name,
    'The percentage of patients with COPD on the register, who have had a review in the preceding 12 months, including a record of the number of exacerbations and an assessment of breathlessness using the Medical Research Council dyspnoea scale.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Chronic obstructive pulmonary disease' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_review_date,
    latest_mrc_dyspnoea_date,
    latest_copd_exacerbation_count_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
