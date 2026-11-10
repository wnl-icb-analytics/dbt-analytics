{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['polypharmacy']
    )
}}

-- One row per person and current repeat product in the NHSBSA chapter scope.
SELECT
    mo.person_id,
    mo.mapped_concept_code,
    bnf.bnf_code,
    bnf.bnf_chapter,
    bnf.bnf_name,
    mo.latest_order_date,
    mo.latest_duration,
    mo.effective_duration_days,
    mo.is_duration_imputed,
    mo.supply_end_date,
    bnf.bnf_chemical_code
FROM {{ ref('int_medication_orders_repeat_current') }} mo
INNER JOIN {{ ref('int_polypharmacy_medications_list') }} bnf
    ON mo.mapped_concept_code = bnf.snomed_code
