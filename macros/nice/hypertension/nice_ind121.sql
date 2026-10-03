{#-
    Calculate NICE IND121 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND121 detail columns, one person per reporting_date.
-#}
{% macro nice_ind121(reference='current') %}
-- NICE IND121: https://www.nice.org.uk/indicators/ind121
-- Urine ACR within three months either side of hypertension diagnosis in the financial year for adults over 18, capped at the reporting date.
WITH population AS (
    {{ nice_hypertension_new_diagnosis_population(reference) }}
    AND population.age > 18
),

evidence AS (
    SELECT person_id, clinical_effective_date::DATE AS test_date
    FROM {{ ref('int_urine_acr_all') }}
    WHERE is_acr_ratio
),

selected AS (
    SELECT population.person_id, population.reporting_date, MAX(evidence.test_date) AS latest_acr_date
    FROM population
    LEFT JOIN evidence
        ON population.person_id = evidence.person_id
        AND evidence.test_date BETWEEN DATEADD(month, -3, population.diagnosis_date)
            AND LEAST(DATEADD(month, 3, population.diagnosis_date), population.reporting_date)
    GROUP BY population.person_id, population.reporting_date
)

SELECT
    population.person_id,
    'IND121' AS indicator_id,
    'Hypertension: urinary albumin for target organ damage' AS indicator_name,
    population.reporting_date,
    {{ nice_financial_year_start('population.reporting_date') }} AS measurement_period_start,
    population.age,
    'Hypertension newly diagnosed in the financial year, aged over 18' AS condition_name,
    {{ nice_practice_columns('population', reference) }},
    population.diagnosis_date,
    selected.latest_acr_date,
    TRUE AS is_in_denominator,
    selected.latest_acr_date IS NOT NULL AS is_in_numerator,
    IFF(selected.latest_acr_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM population
LEFT JOIN selected
    ON population.person_id = selected.person_id
    AND population.reporting_date = selected.reporting_date
{% endmacro %}
