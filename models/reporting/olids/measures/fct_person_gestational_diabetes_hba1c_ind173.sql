{{ config(materialized='view') }}

-- NICE IND173: https://www.nice.org.uk/indicators/ind173
-- HbA1c in 12 months for women with gestational diabetes diagnosed in the last 12 months.
WITH indicator_population AS (
    SELECT
        gdm.person_id,
        age.age
    FROM {{ ref('fct_person_gestational_diabetes_register') }} AS gdm
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON gdm.person_id = age.person_id
    WHERE gdm.is_on_register
        -- NICE excludes women diagnosed more than 12 months ago
        AND gdm.latest_diagnosis_date::DATE >= DATEADD(month, -12, CURRENT_DATE())
),

-- Latest qualifying record in the period
latest_record AS (
    SELECT
        obs.person_id,
        MAX(obs.clinical_effective_date::DATE) AS latest_record_date
    FROM {{ ref('int_hba1c_all') }} AS obs
    INNER JOIN indicator_population AS population
        ON obs.person_id = population.person_id
    WHERE obs.clinical_effective_date::DATE
        BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
    GROUP BY obs.person_id
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
    'IND173' AS indicator_id,
    'Diabetes: gestational diabetes annual HbA1c test' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Gestational diabetes diagnosed in the last 12 months' AS condition_name,
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
