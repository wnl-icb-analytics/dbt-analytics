{% macro nice_ind173(reference='current') %}
{#-
    Calculate NICE IND173 at each reference date using its existing rule.
    Args: reference is current or by_month.
    Returns: the indicator detail columns, one eligible person per reporting_date.
-#}
-- NICE IND173: https://www.nice.org.uk/indicators/ind173
-- HbA1c in 12 months for women whose latest gestational diabetes episode is more than 12 months old, excluding diabetes diagnosed more than 12 months ago (NICE pilot report reading).
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('GESTDIAB', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
    -- The latest episode is more than 12 months old; recent pregnancies have separate monitoring.
    WHERE register.latest_diagnosis_date::DATE < DATEADD(month, -12, population.reporting_date)
        -- A later resolved diabetes code does not lift the diagnosis-history exclusion.
        AND NOT EXISTS (
            SELECT 1
            FROM {{ ref('int_diabetes_diagnoses_all') }} AS diabetes
            WHERE diabetes.person_id = population.person_id
                AND diabetes.is_diagnosis_code
                AND diabetes.clinical_effective_date::DATE < DATEADD(month, -12, population.reporting_date)
                AND {{ ltc_register_known_by('diabetes.clinical_effective_date', 'diabetes.date_recorded', 'population.reporting_date') }}
        )
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        {{ nice_practice_columns('population', reference) }},
        -- Test presence includes value-free tests; a numeric result is not required.
        hba.latest_hba1c_date AS latest_record_date
    FROM indicator_population AS population
    LEFT JOIN {{ nice_ref('int_nice_hba1c_evidence', reference) }} AS hba
        ON population.person_id = hba.person_id
        AND population.reporting_date = hba.reporting_date
        AND hba.latest_hba1c_date >= DATEADD(month, -12, population.reporting_date)
)

SELECT
    person_id,
    'IND173' AS indicator_id,
    'Diabetes: gestational diabetes annual HbA1c test' AS indicator_name,
    reporting_date AS reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'History of gestational diabetes, latest episode more than 12 months ago' AS condition_name,
    {{ nice_practice_columns(none, reference) }},
    latest_record_date,
    TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    CASE
        WHEN latest_record_date IS NOT NULL THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
