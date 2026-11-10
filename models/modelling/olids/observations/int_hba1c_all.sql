{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All HbA1c measurements from observations.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Handles both IFCC and DCCT measurement types with proper unit tracking.
*/

WITH base_observations AS (

    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.date_recorded,
        -- Handle extreme values gracefully - cap at reasonable HbA1c ranges
        CASE 
            WHEN TRY_CAST(obs.result_value AS FLOAT) > 1000 THEN NULL  -- Likely data error
            WHEN TRY_CAST(obs.result_value AS FLOAT) < 0 THEN NULL     -- Negative values invalid
            ELSE CAST(TRY_CAST(obs.result_value AS FLOAT) AS NUMBER(10,2))
        END AS hba1c_original_value,
        obs.result_unit_display,
        obs.result_unit_code AS original_result_unit_code,
        obs.result_unit_display AS original_result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value,

        -- Single measurement type determination - mutually exclusive
        -- Unambiguous ranges determined by value: DCCT is always <20%, IFCC is always >25 mmol/mol.
        -- The 20-25 zone is ambiguous (could be low-normal IFCC or extreme DCCT),
        -- so falls back to the recorded UCUM unit, then the concept cluster.
        CASE
            WHEN TRY_CAST(obs.result_value AS FLOAT) > 25 THEN 'IFCC'
            -- Values from 13 to below 20 recorded as mmol/mol are below the NDA IFCC range
            -- (20-195) and extreme on the DCCT scale, so they stay IFCC and fail validity.
            WHEN TRY_CAST(obs.result_value AS FLOAT) >= 13 AND obs.result_unit_code = 'mmol/mol' THEN 'IFCC'
            WHEN TRY_CAST(obs.result_value AS FLOAT) < 20 THEN 'DCCT'
            -- Ambiguous 20-25 range and value-free tests use unit, then cluster.
            WHEN obs.result_unit_code = 'mmol/mol' THEN 'IFCC'
            WHEN obs.result_unit_code IN ('%', '%{Hemoglobin}') THEN 'DCCT'
            WHEN obs.cluster_id = 'IFCCHBAM_COD' THEN 'IFCC'
            WHEN obs.cluster_id = 'DCCTHBA1C_COD' THEN 'DCCT'
            ELSE 'UNKNOWN'
        END AS measurement_type
    FROM ({{ get_observations("'IFCCHBAM_COD', 'DCCTHBA1C_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
    AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
      
)

,

standardised AS (
    /* Convert all values to IFCC (mmol/mol) using NGSP/DCCT ↔ IFCC relationship
       Formulae (IFCC mmol/mol ↔ NGSP %):
       NGSP% = 0.09148 * IFCC + 2.152
       IFCC  = 10.929 * NGSP% - 23.5
    */
    SELECT
        bo.*,
        CASE
            WHEN bo.measurement_type = 'IFCC' THEN bo.hba1c_original_value
            WHEN bo.measurement_type = 'DCCT' THEN ROUND(10.929 * bo.hba1c_original_value - 23.5, 0)
            ELSE NULL
        END AS hba1c_ifcc,
        CASE
            WHEN bo.measurement_type = 'DCCT' THEN bo.hba1c_original_value
            WHEN bo.measurement_type = 'IFCC' THEN ROUND((0.09148 * bo.hba1c_original_value) + 2.152, 1)
            ELSE NULL
        END AS hba1c_dcct
    FROM base_observations bo
),

assessed AS (
    SELECT
        *,
        CASE
            -- Keep sub-20 recorded mmol/mol results invalid before display rounding.
            WHEN original_result_unit_code = 'mmol/mol'
                AND TRY_CAST(original_result_value AS FLOAT) >= 13
                AND TRY_CAST(original_result_value AS FLOAT) < 20 THEN FALSE
            WHEN measurement_type = 'IFCC' AND hba1c_original_value BETWEEN 20 AND 200 THEN TRUE
            WHEN measurement_type = 'DCCT' AND hba1c_original_value BETWEEN 3 AND 20 THEN TRUE
            ELSE FALSE
        END AS is_valid_hba1c,
        CASE WHEN measurement_type = 'UNKNOWN' THEN NULL
            ELSE COALESCE(
                CASE WHEN original_result_unit_code = 'mmol/mol' THEN 'IFCC'
                    WHEN original_result_unit_code IN ('%', '%{Hemoglobin}') THEN 'DCCT'
                END <> measurement_type, FALSE)
                OR COALESCE(
                    CASE source_cluster_id WHEN 'IFCCHBAM_COD' THEN 'IFCC'
                        WHEN 'DCCTHBA1C_COD' THEN 'DCCT'
                    END <> measurement_type, FALSE)
        END AS is_unit_conflict
    FROM standardised
)

SELECT
    person_id,
    ID,
    clinical_effective_date,
    date_recorded,
    hba1c_original_value,
    hba1c_ifcc,
    hba1c_dcct,
    /* Canonicalise units by measurement type: IFCC → mmol/mol, DCCT → %.
       Override NULL/blank/UNKNOWN/HbA1c or mismatched units. */
    CASE
        WHEN measurement_type = 'IFCC' THEN 'mmol/mol'
        WHEN measurement_type = 'DCCT' THEN '%'
        ELSE COALESCE(NULLIF(TRIM(result_unit_display), ''), 'UNKNOWN')
    END AS result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    measurement_type,
    (measurement_type = 'IFCC') AS is_ifcc,
    (measurement_type = 'DCCT') AS is_dcct,
    original_result_value,
    original_result_unit_code,
    original_result_unit_display,

    -- Display both standardised values with units
    CAST(hba1c_ifcc AS VARCHAR) || ' mmol/mol (' ||
        CAST(hba1c_dcct AS VARCHAR) || '%)' AS hba1c_display,

    is_valid_hba1c,
    is_unit_conflict,

    -- NICE-aligned clinical categorisation using standardised IFCC value
    CASE
        WHEN NOT is_valid_hba1c THEN 'Invalid'
        WHEN hba1c_ifcc < 42 THEN 'Normal'                -- < 6.0%
        WHEN hba1c_ifcc >= 42 AND hba1c_ifcc < 48 THEN 'Prediabetes' -- 42.0–47.9
        WHEN hba1c_ifcc >= 48 AND hba1c_ifcc < 54 THEN 'Diabetes - At NICE Target' -- 48.0–53.9
        WHEN hba1c_ifcc >= 54 AND hba1c_ifcc <= 58 THEN 'Diabetes - Elevated (within QOF)' -- 54.0–58.0 (QOF DM020 inclusive)
        WHEN hba1c_ifcc > 58 AND hba1c_ifcc < 75 THEN 'Diabetes - Above Target' -- >58.0–74.9
        WHEN hba1c_ifcc >= 75 AND hba1c_ifcc < 86 THEN 'Diabetes - High Risk' -- 75.0–85.9
        WHEN hba1c_ifcc >= 86 THEN 'Diabetes - Very High Risk'
        ELSE 'Invalid'
    END AS hba1c_category,

    -- Diabetes diagnostic flag
    CASE
        WHEN is_valid_hba1c THEN hba1c_ifcc >= 48
    END AS indicates_diabetes,

    -- Numerical <=58 flag used by QOF DM020 (people without moderate or severe frailty)
    CASE
        WHEN is_valid_hba1c THEN hba1c_ifcc <= 58
    END AS meets_qof_target

FROM assessed

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
