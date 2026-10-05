{% macro nice_ind144(reference='current') %}
{#-
    Calculate NICE IND144 for eligible CKD members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND144 detail columns, one person per reporting_date.
-#}
-- NICE IND144: https://www.nice.org.uk/indicators/ind144
-- Urine ACR or PCR recorded in 12 months on the CKD register (stage 3 to 5 by code).
WITH indicator_population AS (
    SELECT
        ckd.person_id,
        ckd.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_register('CKD', reference) }}) AS ckd
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON ckd.person_id = population.person_id
        AND ckd.reporting_date = population.reporting_date
),

albumin_dates AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS record_date
    FROM {{ ref('int_urine_acr_all') }}
    -- A recorded ACR or PCR test counts without a numeric result.
    WHERE albumin_test_type IN ('ACR', 'PCR')
    GROUP BY person_id, clinical_effective_date::DATE
),

latest_record AS (
    SELECT
        population.person_id,
        population.reporting_date,
        record.record_date AS latest_record_date
    FROM indicator_population AS population
    ASOF JOIN albumin_dates AS record
        MATCH_CONDITION (population.reporting_date >= record.record_date)
        ON population.person_id = record.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        CASE
            WHEN record.latest_record_date >= DATEADD(month, -12, population.reporting_date)
                THEN record.latest_record_date
        END AS latest_record_date
    FROM indicator_population AS population
    LEFT JOIN latest_record AS record
        ON population.person_id = record.person_id
        AND population.reporting_date = record.reporting_date
)

SELECT
    person_id,
    'IND144' AS indicator_id,
    'Kidney conditions: CKD urine albumin:creatinine ratio' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Chronic kidney disease, stages 3 to 5' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_record_date,
    TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    CASE
        WHEN latest_record_date IS NOT NULL THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed

{% endmacro %}
