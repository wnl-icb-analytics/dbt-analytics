{#-
    Calculate NICE IND79 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND79 detail columns, one person per reporting_date.
-#}
{% macro nice_ind79(reference='current') %}
-- NICE IND79: https://www.nice.org.uk/indicators/ind79
-- Thyroid testing within 15 months for adults with learning disability and Down's syndrome, excluding hypothyroidism register members.
WITH eligible AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('LD', reference) }}) AS ld
        ON population.person_id = ld.person_id
        AND population.reporting_date = ld.reporting_date
    LEFT JOIN ({{ nice_register('THY', reference) }}) AS thyroid
        ON population.person_id = thyroid.person_id
        AND population.reporting_date = thyroid.reporting_date
    WHERE population.age >= 18
        AND thyroid.person_id IS NULL
),

downs_syndrome AS (
    SELECT
        eligible.person_id,
        eligible.reporting_date,
        MIN(diagnosis.clinical_effective_date::DATE) AS downs_syndrome_diagnosis_date
    FROM eligible
    INNER JOIN {{ ref('int_downs_syndrome_diagnoses_all') }} AS diagnosis
        ON eligible.person_id = diagnosis.person_id
        AND {{ ltc_known_date('diagnosis.clinical_effective_date', 'diagnosis.date_recorded') }} <= eligible.reporting_date
    GROUP BY eligible.person_id, eligible.reporting_date
),

assessed AS (
    SELECT
        eligible.person_id,
        eligible.reporting_date,
        eligible.age,
        eligible.practice_code,
        eligible.practice_name,
        diagnosis.downs_syndrome_diagnosis_date,
        review.latest_thyroid_function_test_date,
        COALESCE(review.latest_thyroid_function_test_date
            BETWEEN DATEADD(month, -15, eligible.reporting_date) AND eligible.reporting_date, FALSE) AS is_in_numerator
    FROM eligible
    INNER JOIN downs_syndrome AS diagnosis
        ON eligible.person_id = diagnosis.person_id
        AND eligible.reporting_date = diagnosis.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS review
        ON eligible.person_id = review.person_id
        AND eligible.reporting_date = review.reporting_date
)

SELECT
    person_id,
    'IND79' AS indicator_id,
    'Learning disabilities: annual TSH test' AS indicator_name,
    reporting_date,
    DATEADD(month, -15, reporting_date) AS measurement_period_start,
    age,
    'Learning disability with Down''s syndrome' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    downs_syndrome_diagnosis_date,
    latest_thyroid_function_test_date,
    IFF(is_in_numerator, latest_thyroid_function_test_date, NULL) AS latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
