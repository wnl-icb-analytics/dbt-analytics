{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['polypharmacy']
    )
}}

/*
Current polypharmacy status per person.

Person-level snapshot of current polypharmacy medication burden.
Pure polypharmacy measures only - join to demographic tables for reporting.

Polypharmacy definition (NHSBSA standard):
- Current repeat medications in BNF chapters 1-4, 6-10
- Standard polypharmacy: 5+ medications
- Severe/complex polypharmacy: 10+ medications

Grain: One row per person in dim_person with current polypharmacy medications.
*/

SELECT
    poly.person_id,

    -- Polypharmacy measures
    poly.medication_count,
    poly.medication_bnf_list,
    poly.medication_name_list,
    poly.polypharmacy_status_date,

    -- Polypharmacy flags
    poly.is_polypharmacy_5plus,
    poly.is_polypharmacy_10plus,

    -- Medication count bands for reporting
    CASE
        WHEN poly.medication_count BETWEEN 1 AND 4 THEN '1-4'
        WHEN poly.medication_count BETWEEN 5 AND 9 THEN '5-9'
        WHEN poly.medication_count BETWEEN 10 AND 14 THEN '10-14'
        WHEN poly.medication_count >= 15 THEN '15+'
    END AS medication_count_band

FROM {{ ref('int_polypharmacy_current') }} poly
WHERE EXISTS (
    SELECT 1
    FROM {{ ref('dim_person') }} person
    WHERE person.person_id = poly.person_id
)
