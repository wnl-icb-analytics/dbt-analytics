{% macro nice_ind172(reference='current') %}
{#-
    Calculate NICE IND172 at each reference date using its existing rule.
    Args: reference is current or by_month.
    Returns: the indicator detail columns, one eligible person per reporting_date.
-#}
-- NICE IND172: https://www.nice.org.uk/indicators/ind172
-- HbA1c or fasting plasma glucose in 12 months on the NDH register.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('NDH', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
    -- NICE excludes under-18s and unresolved diabetes of any age.
    WHERE population.age >= 18
        AND NOT COALESCE(register.has_unresolved_diabetes, FALSE)
),

candidate_keys AS (
    SELECT DISTINCT person_id
    FROM indicator_population
),

fasting_glucose_daily AS (
    SELECT
        glucose.person_id,
        glucose.clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_blood_glucose_all') }} AS glucose
    INNER JOIN candidate_keys AS candidate
        ON glucose.person_id = candidate.person_id
    WHERE glucose.source_cluster_id = 'FASPLASGLUC_COD'
    GROUP BY glucose.person_id, glucose.clinical_effective_date::DATE
),

selected_glucose AS (
    SELECT
        population.person_id,
        population.reporting_date,
        glucose.event_date
    FROM indicator_population AS population
    ASOF JOIN fasting_glucose_daily AS glucose
        MATCH_CONDITION (population.reporting_date >= glucose.event_date)
        ON population.person_id = glucose.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        {{ nice_practice_columns('population', reference) }},
        -- Test presence includes value-free tests; a numeric result is not required.
        GREATEST_IGNORE_NULLS(
            CASE WHEN hba.latest_hba1c_date >= DATEADD(month, -12, population.reporting_date)
                THEN hba.latest_hba1c_date END,
            CASE WHEN glucose.event_date >= DATEADD(month, -12, population.reporting_date)
                THEN glucose.event_date END
        ) AS latest_record_date
    FROM indicator_population AS population
    LEFT JOIN {{ nice_ref('int_nice_hba1c_evidence', reference) }} AS hba
        ON population.person_id = hba.person_id
        AND population.reporting_date = hba.reporting_date
    LEFT JOIN selected_glucose AS glucose
        ON population.person_id = glucose.person_id
        AND population.reporting_date = glucose.reporting_date
)

SELECT
    person_id,
    'IND172' AS indicator_id,
    'Diabetes: NDH annual HbA1c or FPG test' AS indicator_name,
    reporting_date AS reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Non-diabetic hyperglycaemia' AS condition_name,
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
