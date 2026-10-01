{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest smoking habit observation per person, including SMOK_COD-only evidence.
QOF v51 checks specific status codes on the latest smoking habit date.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    concept_code,
    code_description,
    source_cluster_id,
    is_smoker_code,
    is_ex_smoker_code,
    is_never_smoked_code,
    smoking_status,
    is_current_smoker,
    is_ex_smoker

FROM {{ ref('int_smoking_status_all') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date::DATE DESC,
        CASE source_cluster_id
            WHEN 'LSMOK_COD' THEN 1
            WHEN 'EXSMOK_COD' THEN 2
            WHEN 'NSMOK_COD' THEN 3
            ELSE 4
        END,
        id DESC
) = 1
