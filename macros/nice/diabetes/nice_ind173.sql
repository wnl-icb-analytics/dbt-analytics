{% macro nice_ind173(reference='current') %}
{#-
    Calculate NICE IND173 at each reference date using its existing rule.
    Args: reference is current or by_month.
    Returns: the indicator detail columns, one eligible person per reporting_date.
-#}
-- NICE IND173: https://www.nice.org.uk/indicators/ind173
-- Episode timing follows NICE NM151; diabetes resolution follows QOF NDH register rules.
WITH gestational_population AS (
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
),

known_diabetes AS (
    SELECT
        population.person_id,
        population.reporting_date,
        MIN(IFF(diabetes.is_diagnosis_code, diabetes.clinical_effective_date::DATE, NULL))
            AS earliest_diagnosis_date,
        MAX(IFF(diabetes.is_diagnosis_code, diabetes.clinical_effective_date::DATE, NULL))
            AS latest_diagnosis_date,
        MAX(IFF(diabetes.is_resolved_code, diabetes.clinical_effective_date::DATE, NULL))
            AS latest_resolution_date
    FROM gestational_population AS population
    INNER JOIN {{ ref('int_diabetes_diagnoses_all') }} AS diabetes
        ON diabetes.person_id = population.person_id
        AND {{ ltc_register_known_by('diabetes.clinical_effective_date', 'diabetes.date_recorded', 'population.reporting_date') }}
    GROUP BY population.person_id, population.reporting_date
),

indicator_population AS (
    SELECT population.*
    FROM gestational_population AS population
    LEFT JOIN known_diabetes AS diabetes
        ON population.person_id = diabetes.person_id
        AND population.reporting_date = diabetes.reporting_date
    -- Diabetes diagnosed more than 12 months before excludes unless a later code resolves it;
    -- same-day resolution does not lift it.
    WHERE diabetes.earliest_diagnosis_date IS NULL
        OR diabetes.earliest_diagnosis_date >= DATEADD(month, -12, population.reporting_date)
        OR diabetes.latest_resolution_date > diabetes.latest_diagnosis_date
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
    'The percentage of women who have had gestational diabetes, diagnosed more than 12 months ago, who have had an HbA1c test in the preceding 12 months.' AS indicator_description,
    reporting_date AS reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Gestational diabetes more than 12 months ago' AS denominator_description,
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
