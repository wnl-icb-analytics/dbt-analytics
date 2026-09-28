{{ config(materialized='table', cluster_by=['person_id']) }}

/*
People with any recorded history of haemorrhagic stroke (PCD HSTRK_COD), one row
per person. Undated records count as history. Records whose original date is after
the build date do not. Shared exclusion for secondary-prevention measures.
*/

SELECT
    person_id,
    MIN(clinical_effective_date_raw::DATE) AS earliest_recorded_date,
    BOOLOR_AGG(clinical_effective_date_raw IS NULL) AS has_undated_record,
    COUNT(*) AS record_count
FROM {{ ref('int_haemorrhagic_stroke_diagnoses_all') }}
WHERE clinical_effective_date_raw IS NULL
    OR clinical_effective_date_raw::DATE <= CURRENT_DATE()
GROUP BY person_id
