{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
QOF BMI observations for the obesity register: numeric BMI values (BMIVAL_COD)
and BMI30+ codes (BMI30_COD), with validity and threshold flags.
One row per observation with a usable BMI value.
*/

WITH base_observations AS (

    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.date_recorded,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value,

        -- Extract BMI value from result_value, handling both numeric and coded values
        -- Use TRY_TO_NUMBER to handle invalid numeric values gracefully
        CASE
            WHEN obs.cluster_id = 'BMIVAL_COD' THEN TRY_CAST(obs.result_value AS FLOAT)
            WHEN obs.cluster_id = 'BMI30_COD' THEN 30 -- BMI30_COD implies BMI >= 30
            ELSE NULL
        END AS bmi_value

    FROM ({{ get_observations("'BMI30_COD', 'BMIVAL_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
)

SELECT
    id,
    person_id,
    clinical_effective_date,
    date_recorded,
    concept_code,
    concept_display,
    source_cluster_id,
    result_value,
    bmi_value,

    -- Data quality flags
    bmi_value BETWEEN 5 AND 400 AS is_valid_bmi,

    -- QOF obesity register flags
    source_cluster_id = 'BMI30_COD' OR bmi_value >= 30 AS is_bmi_30_plus,
    bmi_value >= 27.5 AS is_bmi_27_5_plus,
    bmi_value >= 25 AS is_bmi_25_plus

FROM base_observations
WHERE bmi_value IS NOT NULL
