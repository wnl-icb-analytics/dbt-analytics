{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest National Data Opt-Out preference per person.
Joins NATIONAL_DATA_OPT_OUT to PATIENT on sk_patient_id to resolve person_id.
One row per (person_id, preference_type) with is_opted_in derived from the latest record.
*/

WITH ndoo_resolved AS (
    SELECT
        p.person_id,
        n.preference_type,
        n.preference_status,
        n.effective_from,
        n.effective_to,
        n.lds_record_id
    FROM {{ ref('stg_olids_national_data_opt_out') }} n
    INNER JOIN {{ ref('stg_olids_patient') }} p
        ON n.sk_patient_id = p.sk_patient_id
    WHERE n.is_latest = TRUE
        AND p.person_id IS NOT NULL
)

SELECT
    person_id,
    preference_type,
    preference_status,
    -- REVIEW: assumes OPT_IN means secondary use is allowed for this preference type.
    preference_status = 'OPT_IN' AS is_opted_in,
    effective_from,
    effective_to
FROM ndoo_resolved
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id, preference_type
    -- REVIEW: is_latest is upstream-defined; dates and record id resolve duplicate latest rows.
    ORDER BY effective_from DESC NULLS LAST, effective_to DESC NULLS LAST, lds_record_id DESC
) = 1
