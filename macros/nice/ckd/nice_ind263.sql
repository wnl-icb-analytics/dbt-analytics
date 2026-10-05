{% macro nice_ind263(reference='current') %}
{#-
    Calculate NICE IND263 for eligible CKD members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND263 detail columns, one person per reporting_date.
-#}
-- NICE IND263: https://www.nice.org.uk/indicators/ind263
-- ACE inhibitor or ARB order in 6 months for people on the CKD register with a latest ACR of 70 mg/mmol or more and no diabetes.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        profile.reporting_date,
        profile.has_diabetes,
        profile.latest_acr_value,
        profile.latest_egfr_value,
        profile.latest_ras_order_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM {{ nice_ref('int_ckd_profile', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE profile.latest_acr_value >= 70
        AND NOT profile.has_diabetes
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_acr_value,
        population.latest_egfr_value,
        population.latest_ras_order_date AS latest_therapy_order_date,
        CASE
            WHEN population.latest_ras_order_date >= DATEADD(month, -6, population.reporting_date)
                THEN population.latest_ras_order_date
        END AS latest_record_date,
        COALESCE(population.latest_ras_order_date >= DATEADD(month, -6, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND263' AS indicator_id,
    'Kidney conditions: CKD - ACEi and ARB' AS indicator_name,
    reporting_date,
    DATEADD(month, -6, reporting_date) AS measurement_period_start,
    age,
    'Kidney disease with heavy albuminuria, without diabetes' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    latest_acr_value,
    latest_egfr_value,
    latest_therapy_order_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_therapy_order_date IS NULL THEN 'NEVER_TREATED'
        ELSE 'NOT_TREATED_IN_PERIOD'
    END AS indicator_status
FROM assessed AS result

{% endmacro %}
