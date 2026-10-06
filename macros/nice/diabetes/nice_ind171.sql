{% macro nice_ind171(reference='current') %}
{#-
    Calculate NICE IND171 at each reference date using its reviewed rule.
    Args: reference is current or by_month.
    Returns: the indicator detail columns, one eligible person per reporting_date.
-#}
-- NICE IND171: https://www.nice.org.uk/indicators/ind171
-- Referral (made or declined) to the NHS Diabetes Prevention Programme for adults newly diagnosed with non-diabetic hyperglycaemia in the preceding 12 months, excluding unresolved diabetes.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        register.earliest_diagnosis_date::DATE AS diagnosis_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('NDH', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
    WHERE register.earliest_diagnosis_date::DATE
        BETWEEN DATEADD(month, -12, population.reporting_date) AND population.reporting_date
        AND population.age >= 18
        AND NOT COALESCE(register.has_unresolved_diabetes, FALSE)
),

first_referral AS (
    SELECT
        population.person_id,
        population.reporting_date,
        MIN(event.clinical_effective_date::DATE) AS latest_record_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_referral_ndpp_all') }} AS event
        ON population.person_id = event.person_id
        AND event.clinical_effective_date::DATE
            BETWEEN population.diagnosis_date AND population.reporting_date
        -- Made or declined referrals count; invitations do not.
        AND event.concept_code IN ('1025321000000109', '1025301000000100')
    GROUP BY population.person_id, population.reporting_date
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        {{ nice_practice_columns('population', reference) }},
        population.diagnosis_date,
        referral.latest_record_date,
        referral.person_id IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    LEFT JOIN first_referral AS referral
        ON population.person_id = referral.person_id
        AND population.reporting_date = referral.reporting_date
)

SELECT
    person_id,
    'IND171' AS indicator_id,
    'Diabetes: NDH diabetes prevention programme' AS indicator_name,
    'The percentage of patients newly diagnosed with non-diabetic hyperglycaemia in the preceding 12 months who have been referred to a Healthier You: NHS Diabetes Prevention Programme for intensive lifestyle advice.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'New non-diabetic hyperglycaemia, aged 18 or over' AS denominator_description,
    {{ nice_practice_columns(none, reference) }},
    diagnosis_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
