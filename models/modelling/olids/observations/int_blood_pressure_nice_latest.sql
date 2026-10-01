{{ config(materialized='table', cluster_by=['person_id']) }}

-- NICE assesses the latest complete pair, including an implausible latest pair.
WITH reading_rows AS (
    SELECT
        person_id,
        effective_date::DATE AS reading_date,
        COALESCE(parent_observation_id::VARCHAR, 'NOPARENT') AS reading_id,
        result_value,
        is_systolic_row,
        is_diastolic_row,
        is_home_bp_row,
        is_abpm_bp_row
    FROM {{ ref('int_blood_pressure_observations_base') }}
    WHERE effective_date::DATE <= CURRENT_DATE()
),

-- Preserve the existing date-level MAX/MAX fallback for unparented components.
complete_readings AS (
    SELECT
        person_id,
        reading_date,
        reading_id,
        MAX(CASE WHEN is_systolic_row THEN result_value END) AS systolic_value,
        MAX(CASE WHEN is_diastolic_row THEN result_value END) AS diastolic_value,
        BOOLOR_AGG(is_home_bp_row) AS is_home_bp_event,
        BOOLOR_AGG(is_abpm_bp_row) AS is_abpm_bp_event
    FROM reading_rows
    GROUP BY person_id, reading_date, reading_id
    HAVING systolic_value IS NOT NULL AND diastolic_value IS NOT NULL
),

assessed AS (
    SELECT
        *,
        systolic_value BETWEEN 40 AND 350
            AND diastolic_value BETWEEN 20 AND 200 AS is_valid_bp
    FROM complete_readings
)

SELECT
    person_id,
    reading_date AS latest_bp_date,
    systolic_value AS latest_systolic_value,
    diastolic_value AS latest_diastolic_value,
    is_valid_bp,
    is_home_bp_event,
    is_abpm_bp_event,
    CASE
        WHEN is_home_bp_event OR is_abpm_bp_event THEN 'HBPM_ABPM'
        ELSE 'CLINIC'
    END AS applied_measurement_context
FROM assessed
-- Dates define recency. On that date, prefer valid pairs and keep lowest-of-day.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY reading_date DESC, is_valid_bp DESC,
        systolic_value ASC, diastolic_value ASC, reading_id ASC
) = 1
