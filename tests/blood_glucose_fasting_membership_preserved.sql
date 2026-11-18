-- A fasting test must remain identifiable after overlapping cluster membership.
SELECT glucose.person_id, glucose.concept_code, glucose.clinical_effective_date
FROM {{ ref('int_blood_glucose_all') }} AS glucose
WHERE glucose.source_cluster_id <> 'FASPLASGLUC_COD'
    AND EXISTS (
        SELECT 1
        FROM {{ ref('stg_reference_combined_codesets') }} AS codes
        WHERE codes.cluster_id = 'FASPLASGLUC_COD'
            AND codes.code = glucose.concept_code
    )
