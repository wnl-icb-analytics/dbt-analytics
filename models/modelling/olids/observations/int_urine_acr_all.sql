{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Urine albumin testing observations from the PCD cluster NDAALB_COD, kept with or
without a numeric result. albumin_test_type splits the cluster by concept code;
acr_value and its categories apply to ACR observations only.

Deliberately OTHER: 252242007 nicotinic acid loading test and 30711000237105
retinol-binding protein:creatinine ratio, which are in the cluster but are not
albumin tests. Profile and panel codes (412902007, 994621000000103,
194961000000101) are typed URINE_ALBUMIN because they record that albumin was
measured. tests/urine_albumin_codes_classified.sql fails if a new cluster member
lands in OTHER.
*/

WITH base_observations AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.result_value AS original_result_value,
        TRY_CAST(obs.result_value AS FLOAT) AS numeric_result_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        CASE
            WHEN obs.mapped_concept_code IN (
                '1023491000000104',  -- Urine albumin:creatinine ratio
                '1028271000000109',  -- Albumin/creatinine ratio
                '5801000237100',     -- Mass concentration ratio of albumin to creatinine in urine
                '1027791000000103',  -- Urine microalbumin/creatinine ratio
                '149861000000104'    -- ACR by test strip, semi-quantitative
            ) THEN 'ACR'
            WHEN obs.mapped_concept_code IN (
                '1006631000000104',  -- Random urine protein:creatinine ratio
                '1028731000000100',  -- Urine protein/creatinine ratio
                '58201000237103',    -- Mass concentration ratio of protein to creatinine in urine
                '1108311000000109'   -- Ratio of protein to creatinine in 24 hour urine specimen
            ) THEN 'PCR'
            WHEN obs.mapped_concept_code IN (
                '1003301000000109',  -- Urine albumin level
                '1010251000000109',  -- Urine microalbumin level
                '6311000237102',     -- Mass concentration of albumin in urine
                '3321000237102',     -- Mass concentration of microalbumin in urine
                '104486009',         -- Albumin measurement, urine, quantitative
                '104819000',         -- Microalbumin measurement, urine, quantitative
                '104820006',         -- Microalbumin measurement, urine, semi-quantitative
                '271000000',         -- Urine albumin measurement
                '46716003',          -- Microalbuminuria measurement
                '412902007',         -- Urine microalbumin profile (procedure)
                '994621000000103',   -- Urine microalbumin profile (observable)
                '194961000000101'    -- Microalbumin profile
            ) THEN 'URINE_ALBUMIN'
            WHEN obs.mapped_concept_code IN (
                '1019891000000109',  -- 24 hour urine albumin output
                '313502007',         -- 24 hour urine albumin output measurement
                '1028571000000106',  -- 24 hour urine protein excretion test
                '1016051000000101',  -- 24 hour urine protein output
                '1000941000000101',  -- Albumin excretion rate
                '6131000237105',     -- Mass concentration of albumin excretion rate in urine
                '5121000237101',     -- Mass concentration of albumin in 24 hour urine
                '3361000237108',     -- Overnight albumin excretion rate concentration
                '1002901000000100',  -- Overnight albumin excretion rate
                '29809003',          -- Microalbuminuria measurement, 10-hour collection
                '45590004'           -- Microalbuminuria measurement, 24-hour collection
            ) THEN 'TIMED_ALBUMIN'
            WHEN obs.mapped_concept_code IN (
                '312975006',         -- Microalbuminuria
                '762261001',         -- Persistent albuminuria
                '18521000119106',    -- Microalbuminuria due to type 1 diabetes
                '90781000119102',    -- Microalbuminuria due to type 2 diabetes
                '236499007',         -- Microalbuminuric nephropathy due to diabetes
                '421305000',         -- Persistent microalbuminuria due to type 1 diabetes
                '420715001',         -- Persistent microalbuminuria due to type 2 diabetes
                '401110002'          -- Type 1 diabetes with persistent microalbuminuria
            ) THEN 'ALBUMINURIA_FINDING'
            ELSE 'OTHER'
        END AS albumin_test_type
    FROM ({{ get_observations("'NDAALB_COD'", source='PCD') }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

valued AS (
    SELECT
        *,
        albumin_test_type = 'ACR' AS is_acr_ratio,
        numeric_result_value IS NOT NULL AS is_result_recorded,
        CASE
            WHEN albumin_test_type = 'ACR'
                AND numeric_result_value BETWEEN 0 AND 9999.99
                THEN numeric_result_value
        END AS acr_value
    FROM base_observations
)

SELECT
    person_id,
    id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    albumin_test_type,
    is_acr_ratio,
    is_result_recorded,
    original_result_value,
    result_unit_display,
    acr_value,
    acr_value IS NOT NULL AS is_valid_acr,

    -- Clinical categorisation (mg/mmol) for ACR results
    CASE
        WHEN acr_value IS NULL AND is_acr_ratio AND is_result_recorded THEN 'Invalid'
        WHEN acr_value < 3 THEN 'Normal (<3)'
        WHEN acr_value < 30 THEN 'Mildly Increased (3-30)'
        WHEN acr_value < 300 THEN 'Moderately Increased (30-300)'
        WHEN acr_value >= 300 THEN 'Severely Increased (≥300)'
    END AS acr_category,
    COALESCE(acr_value >= 3, FALSE) AS is_acr_elevated,
    COALESCE(acr_value >= 3 AND acr_value < 30, FALSE) AS is_microalbuminuria,
    COALESCE(acr_value >= 30, FALSE) AS is_macroalbuminuria

FROM valued
