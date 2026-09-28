{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid HbA1c measurement per person.
Uses the comprehensive int_hba1c_all model and filters to most recent valid HbA1c.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    hba1c_original_value,
    hba1c_ifcc,
    hba1c_dcct,
    concept_code,
    concept_display,
    source_cluster_id,
    is_ifcc,
    is_dcct,
    result_unit_display,
    hba1c_display,
    hba1c_category,
    indicates_diabetes,
    meets_qof_target,
    original_result_value

FROM {{ ref('int_hba1c_all') }}

WHERE is_valid_hba1c = TRUE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, id DESC
) = 1
