{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid HbA1c measurement per person.
Uses the comprehensive int_hba1c_all model and filters to most recent valid HbA1c.
*/

WITH results AS (
    SELECT
        *,
        MAX(clinical_effective_date) OVER (PARTITION BY person_id) AS most_recent_result_date
    FROM {{ ref('int_hba1c_all') }}
)

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
    original_result_value,
    original_result_unit_code,
    is_unit_conflict,
    most_recent_result_date,
    most_recent_result_date > clinical_effective_date AS has_later_unassessable_result

FROM results

WHERE is_valid_hba1c = TRUE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, id DESC
) = 1
