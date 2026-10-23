{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['polypharmacy']
    )
}}

/*
Current polypharmacy medication counts and flags.

Calculates current state directly from int_polypharmacy_medications_current
(source of truth for current medications) rather than relying on historical
SCD table which uses smoothing and may not reflect exact current state.

Polypharmacy thresholds:
- 5+ medications: Standard polypharmacy threshold
- 10+ medications: Severe/complex polypharmacy threshold

Grain: One row per person with current polypharmacy-scope medications
*/

WITH current_medication_details AS (
    -- Count and aggregate current medications directly
    SELECT
        person_id,
        COUNT(DISTINCT mapped_concept_code) AS medication_count,
        ARRAY_AGG(DISTINCT bnf_code) WITHIN GROUP (ORDER BY bnf_code) AS medication_bnf_list,
        -- Filter out NULL bnf_name values to ensure array contains only valid strings
        -- Using ARRAY_COMPACT to remove NULLs after aggregation
        ARRAY_COMPACT(ARRAY_AGG(DISTINCT bnf_name) WITHIN GROUP (ORDER BY bnf_name)) AS medication_name_list,
        MIN(latest_order_date) AS earliest_current_order_date
    FROM {{ ref('int_polypharmacy_medications_current') }}
    GROUP BY person_id
)

SELECT
    person_id,
    medication_count,
    medication_bnf_list,
    medication_name_list,
    medication_count >= 5 AS is_polypharmacy_5plus,
    medication_count >= 10 AS is_polypharmacy_10plus,
    earliest_current_order_date AS polypharmacy_status_date
FROM current_medication_details
