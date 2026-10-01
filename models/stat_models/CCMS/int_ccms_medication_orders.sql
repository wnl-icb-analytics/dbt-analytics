{{
    config(
        materialized='view')
}}

/*
Medication orders matching the CCMS dm+d codelist, pre-filtered to the windows
the Cambridge Comorbidity Score uses.

Grain: one row per medication order that maps to a CCMS condition.

Filter:
- Last 12 months for all CCMS conditions
- Lithium (conditionid 5029) — any prescription ever, no date constraint
  (per Payne et al. 2020 / 2022 modified CCMS)

The codelist join collapses ~all medication orders down to just CCMS-relevant
ones, so a view is fine — no need to materialise.
*/

SELECT
    mo.person_id,
    mo.id AS medication_order_id,
    mo.clinical_effective_date::DATE AS order_date,
    mo.mapped_concept_code,
    d.conditionid,
    d.conditionname
FROM {{ ref('stg_olids_medication_order') }} mo
INNER JOIN {{ ref('stg_common_ccmc_dmd') }} d
    ON mo.mapped_concept_code = d.productid::VARCHAR
WHERE mo.clinical_effective_date IS NOT NULL
    AND (
        mo.clinical_effective_date >= DATEADD('month', -12, CURRENT_DATE())
        OR d.conditionid = 5029  -- lithium: ignore time window
    )
