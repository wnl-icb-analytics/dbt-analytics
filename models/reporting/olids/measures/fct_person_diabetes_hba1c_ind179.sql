{{ config(materialized='view') }}

-- NICE IND179: https://www.nice.org.uk/indicators/ind179
-- Last HbA1c in 12 months at or below 58 mmol/mol, diabetes register without moderate or severe frailty.
WITH indicator_population AS (
    SELECT
        diabetes.person_id,
        age.age,
        frailty.latest_frailty_severity
    FROM {{ ref('fct_person_diabetes_register') }} AS diabetes
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON diabetes.person_id = age.person_id
    LEFT JOIN {{ ref('fct_person_frailty_register') }} AS frailty
        ON diabetes.person_id = frailty.person_id
    WHERE diabetes.is_on_register
        AND COALESCE(frailty.latest_frailty_severity, 'None') NOT IN ('Moderate', 'Severe')
        -- NICE exclusions: fructosamine measured instead of HbA1c, or on maximum tolerated treatment, in the period
        AND NOT EXISTS (
            SELECT 1
            FROM {{ ref('int_fructosamine_all') }} AS fructosamine
            WHERE fructosamine.person_id = diabetes.person_id
                AND fructosamine.clinical_effective_date::DATE >= DATEADD(month, -12, CURRENT_DATE())
        )
        AND NOT EXISTS (
            SELECT 1
            FROM {{ ref('int_diabetes_max_tolerated_treatment_all') }} AS max_tolerated
            WHERE max_tolerated.person_id = diabetes.person_id
                AND max_tolerated.clinical_effective_date::DATE >= DATEADD(month, -12, CURRENT_DATE())
        )
),

-- Last recorded HbA1c in the period; an invalid last result is not replaced by an older one
last_hba1c AS (
    SELECT
        hba.person_id,
        hba.id AS observation_id,
        hba.clinical_effective_date,
        hba.hba1c_ifcc,
        hba.is_valid_hba1c
    FROM {{ ref('int_hba1c_all') }} AS hba
    INNER JOIN indicator_population AS population
        ON hba.person_id = population.person_id
    WHERE hba.clinical_effective_date::DATE
        BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY hba.person_id
        ORDER BY hba.clinical_effective_date DESC, hba.id DESC
    ) = 1
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_frailty_severity,
        hba.observation_id AS latest_hba1c_observation_id,
        hba.clinical_effective_date::DATE AS latest_hba1c_date,
        hba.hba1c_ifcc AS latest_hba1c_value,
        COALESCE(hba.is_valid_hba1c, FALSE) AS is_latest_hba1c_valid,
        58 AS indicator_threshold
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN last_hba1c AS hba
        ON population.person_id = hba.person_id
)

SELECT
    person_id,
    'IND179' AS indicator_id,
    'Diabetes: HbA1c 58 mmol/mol' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Diabetes without moderate or severe frailty' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_frailty_severity,
    latest_hba1c_observation_id,
    latest_hba1c_date AS latest_record_date,
    latest_hba1c_value,
    is_latest_hba1c_valid,
    indicator_threshold,
    'mmol/mol' AS threshold_unit,
    TRUE AS is_in_denominator,
    latest_hba1c_observation_id IS NOT NULL AS is_hba1c_recorded_in_period,
    COALESCE(is_latest_hba1c_valid AND latest_hba1c_value <= indicator_threshold, FALSE) AS is_in_numerator,
    CASE
        WHEN latest_hba1c_observation_id IS NULL THEN 'NOT_RECORDED_IN_PERIOD'
        WHEN NOT is_latest_hba1c_valid THEN 'NOT_ASSESSABLE'
        WHEN latest_hba1c_value <= indicator_threshold THEN 'ACHIEVED'
        ELSE 'ABOVE_TARGET'
    END AS indicator_status
FROM assessed
