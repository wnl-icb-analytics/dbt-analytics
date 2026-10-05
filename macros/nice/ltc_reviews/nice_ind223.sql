{% macro nice_ind223(reference='current') %}
{#-
    Calculate NICE IND223 from dated LTC population and review evidence.
    Args: reference is current or by_month.
    Returns: the IND223 detail projection, one eligible person per reporting_date.
-#}
-- NICE IND223: https://www.nice.org.uk/indicators/ind223
-- Cancer care review within 12 months of the latest new cancer diagnosis for people diagnosed in the preceding 24 months; the register excludes non-melanoma skin cancer.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        profile.latest_cancer_diagnosis_date,
        review.first_cancer_care_review_after_diagnosis_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS review
        ON population.person_id = review.person_id
        AND population.reporting_date = review.reporting_date
    WHERE profile.has_cancer
        AND profile.latest_cancer_diagnosis_date
            BETWEEN DATEADD(month, -24, population.reporting_date) AND population.reporting_date
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_cancer_diagnosis_date AS diagnosis_date,
        -- Retain the first later review as detail when it misses the 12-month deadline.
        population.first_cancer_care_review_after_diagnosis_date AS latest_review_date,
        CASE WHEN population.first_cancer_care_review_after_diagnosis_date <= DATEADD(month, 12, population.latest_cancer_diagnosis_date)
            THEN population.first_cancer_care_review_after_diagnosis_date END AS latest_record_date,
        COALESCE(population.first_cancer_care_review_after_diagnosis_date
            <= DATEADD(month, 12, population.latest_cancer_diagnosis_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND223' AS indicator_id,
    'Cancer: review within 12 months' AS indicator_name,
    reporting_date,
    DATEADD(month, -24, reporting_date) AS measurement_period_start,
    age,
    'Cancer diagnosed in the preceding 24 months' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    diagnosis_date,
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
