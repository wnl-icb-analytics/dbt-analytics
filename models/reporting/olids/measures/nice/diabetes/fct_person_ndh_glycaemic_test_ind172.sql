{{ config(materialized='view') }}

-- NICE IND172: https://www.nice.org.uk/indicators/ind172
-- HbA1c or fasting plasma glucose in 12 months on the NDH register.
WITH indicator_population AS (
    SELECT
        ndh.person_id,
        age.age
    FROM {{ ref('fct_person_ndh_register') }} AS ndh
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON ndh.person_id = age.person_id
    WHERE ndh.is_on_register
),

-- Latest qualifying record in the period
latest_record AS (
    SELECT
        person_id,
        MAX(record_date) AS latest_record_date
    FROM (
        SELECT hba.person_id, hba.clinical_effective_date::DATE AS record_date
        FROM {{ ref('int_hba1c_all') }} AS hba
        INNER JOIN indicator_population AS population
            ON hba.person_id = population.person_id
        UNION ALL
        SELECT glucose.person_id, glucose.clinical_effective_date::DATE
        FROM {{ ref('int_blood_glucose_all') }} AS glucose
        INNER JOIN indicator_population AS population
            ON glucose.person_id = population.person_id
        WHERE glucose.source_cluster_id = 'FASPLASGLUC_COD'
    )
    WHERE record_date BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
    GROUP BY person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        record.latest_record_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN latest_record AS record
        ON population.person_id = record.person_id
)

SELECT
    person_id,
    'IND172' AS indicator_id,
    'Diabetes: NDH annual HbA1c or FPG test' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Non-diabetic hyperglycaemia' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_record_date,
    TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    CASE
        WHEN latest_record_date IS NOT NULL THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
