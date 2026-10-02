{{ config(materialized='table', cluster_by=['person_id', 'reading_date']) }}

-- NICE control evidence: one complete pair per person and clinical day, including invalid-only days.
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

complete_readings AS (
    SELECT
        person_id,
        reading_date,
        reading_id,
        -- Unparented components use plausible maxima, retaining invalid-only evidence.
        CASE WHEN reading_id = 'NOPARENT' THEN
            COALESCE(
                MAX(CASE WHEN is_systolic_row AND result_value BETWEEN 40 AND 350 THEN result_value END),
                MAX(CASE WHEN is_systolic_row THEN result_value END)
            )
        ELSE MAX(CASE WHEN is_systolic_row THEN result_value END)
        END AS systolic_value,
        CASE WHEN reading_id = 'NOPARENT' THEN
            COALESCE(
                MAX(CASE WHEN is_diastolic_row AND result_value BETWEEN 20 AND 200 THEN result_value END),
                MAX(CASE WHEN is_diastolic_row THEN result_value END)
            )
        ELSE MAX(CASE WHEN is_diastolic_row THEN result_value END)
        END AS diastolic_value,
        BOOLOR_AGG(is_home_bp_row) AS is_home_bp_event,
        BOOLOR_AGG(is_abpm_bp_row) AS is_abpm_bp_event
    FROM reading_rows
    GROUP BY person_id, reading_date, reading_id
    HAVING systolic_value IS NOT NULL
        AND diastolic_value IS NOT NULL
),

assessed AS (
    SELECT
        person_id,
        reading_date,
        reading_id,
        systolic_value,
        diastolic_value,
        systolic_value BETWEEN 40 AND 350
            AND diastolic_value BETWEEN 20 AND 200 AS is_valid_bp,
        is_home_bp_event,
        is_abpm_bp_event
    FROM complete_readings
)

SELECT
    person_id,
    reading_date,
    reading_id,
    systolic_value,
    diastolic_value,
    is_valid_bp,
    is_home_bp_event,
    is_abpm_bp_event,
    CASE
        WHEN is_home_bp_event OR is_abpm_bp_event THEN 'HBPM_ABPM'
        ELSE 'CLINIC'
    END AS applied_measurement_context
FROM assessed
-- Select valid pairs first on each day, then the lowest systolic/diastolic and reading id.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id, reading_date
    ORDER BY is_valid_bp DESC,
        systolic_value ASC,
        diastolic_value ASC,
        reading_id ASC
) = 1
