{{ config(materialized='view') }}

-- NICE IND87: https://www.nice.org.uk/indicators/ind87
-- Latest serum lithium recorded in 4 months and in the 0.4 to 1.0 mmol/L range for people on lithium therapy.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.is_on_lithium
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_lithium_level_date AS latest_lithium_level_date,
        population.latest_lithium_level AS latest_lithium_level,
        CASE WHEN population.latest_lithium_level_date >= DATEADD(month, -4, CURRENT_DATE()) THEN population.latest_lithium_level_date END AS latest_record_date,
        COALESCE(population.latest_lithium_level_date >= DATEADD(month, -4, CURRENT_DATE()), FALSE)
            AND population.is_latest_lithium_level_in_range AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND87' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: lithium levels in therapeutic range' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -4, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Lithium therapy (prescribed in the preceding 6 months)' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_lithium_level_date,
    latest_lithium_level,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_record_date IS NULL THEN 'NOT_RECORDED_IN_PERIOD'
        WHEN latest_lithium_level IS NULL THEN 'NOT_ASSESSABLE'
        ELSE 'OUT_OF_RANGE'
    END AS indicator_status
FROM assessed
