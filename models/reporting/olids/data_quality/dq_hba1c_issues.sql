{{
    config(
        materialized='table',
        tags=['data_quality', 'hba1c'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- Diagnose upstream invalid results using NDA limits before display rounding.
WITH observations AS (
    SELECT *, TRY_CAST(original_result_value AS FLOAT) AS numeric_result
    FROM {{ ref('int_hba1c_all') }}
)

SELECT
    person_id,
    ID,
    clinical_effective_date,
    hba1c_original_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    is_ifcc,
    is_dcct,
    original_result_value,
    hba1c_display,
    hba1c_category,
    
    numeric_result IS NOT NULL AND (is_ifcc OR is_dcct)
        AND NOT is_valid_hba1c AS is_hba1c_out_of_range,
    
    CASE 
        WHEN clinical_effective_date IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_date_missing,
    
    CASE 
        WHEN clinical_effective_date > CURRENT_DATE THEN TRUE 
        ELSE FALSE 
    END AS is_future_date,
    
    CASE 
        WHEN (is_ifcc AND hba1c_original_value > 150) OR
             (is_dcct AND hba1c_original_value > 15) THEN TRUE 
        ELSE FALSE 
    END AS is_extreme_outlier,
    
    CASE 
        WHEN (is_ifcc = FALSE AND is_dcct = FALSE) OR
             (is_ifcc = TRUE AND is_dcct = TRUE) THEN TRUE 
        ELSE FALSE 
    END AS is_measurement_type_ambiguous,
    
    CASE
        WHEN NOT is_valid_hba1c AND is_ifcc AND numeric_result < 20 THEN 'IFCC Below Valid Range'
        WHEN NOT is_valid_hba1c AND is_ifcc AND numeric_result >= 20 THEN 'IFCC Above Valid Range'
        WHEN NOT is_valid_hba1c AND is_dcct AND numeric_result < 20 THEN 'DCCT Below Valid Range'
        WHEN NOT is_valid_hba1c AND is_dcct AND numeric_result >= 20 THEN 'DCCT Above Valid Range'
        WHEN is_ifcc AND hba1c_original_value > 150 THEN 'IFCC Extremely High (> 150)'
        WHEN is_dcct AND hba1c_original_value > 15 THEN 'DCCT Extremely High (> 15)'
        WHEN (is_ifcc = FALSE AND is_dcct = FALSE) THEN 'Measurement Type Unknown'
        WHEN (is_ifcc = TRUE AND is_dcct = TRUE) THEN 'Measurement Type Ambiguous'
        ELSE hba1c_category
    END AS hba1c_category_with_issues

FROM observations

-- Only include observations with DQ issues
WHERE original_result_value IS NOT NULL
  AND (NOT is_valid_hba1c
       OR is_date_missing OR is_future_date OR is_measurement_type_ambiguous)

ORDER BY person_id, clinical_effective_date DESC
