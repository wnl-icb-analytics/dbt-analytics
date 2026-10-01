{{
    config(
        materialized='view'
    )
}}

/*
Medication orders enriched with prescription (statement) details + BNF hierarchy.
View — not materialised.

Grain: one row per medication order (issue).

Joins:
- stg_olids_medication_statement: prescription details (acute/repeat, active status, expiry)
- int_patient_person_unique: patient_id → person_id

BNF hierarchy comes from upstream stg_olids_medication_order (pre-joined in dbt-olids,
clustered on bnf_chapter):
- bnf_code: full BNF code (e.g. 0212000B0AAAAAA)
- bnf_chapter: 2-digit chapter (e.g. 02 = Cardiovascular)
- bnf_section: 4-digit section (e.g. 0212 = Lipid-Regulating Drugs)
- bnf_paragraph: 6-digit paragraph (computed inline from bnf_code)
- bnf_name: human-readable BNF product name
*/

SELECT
    -- Order identifiers
    mo.id AS medication_order_id,
    mo.medication_statement_id,
    pp.person_id,
    mo.patient_id,
    mo.provider_organisation_id,
    mo.publisher_organisation_code AS practice_code,

    -- Order details
    mo.clinical_effective_date AS order_date,
    mo.medication_name,
    mo.mapped_concept_code,
    mo.mapped_concept_display,
    mo.dose,
    mo.quantity_value,
    mo.quantity_unit,
    mo.duration_days,
    mo.estimated_cost,
    mo.age_at_event,

    -- BNF classification (pre-computed upstream in dbt-olids)
    mo.bnf_code,
    mo.bnf_name,
    mo.bnf_chapter,
    mo.bnf_section,
    LEFT(mo.bnf_code, 6) AS bnf_paragraph,

    -- Prescription (statement) details
    -- authorisation_type_source_code is the clean classification: Acute, Repeat, Repeat Dispensing, Automatic
    ms.authorisation_type_source_code AS issue_type,
    ms.is_active AS prescription_is_active,
    ms.cancellation_date AS prescription_cancellation_date,
    ms.expiry_date AS prescription_expiry_date,

    -- Issue method (how the order was issued)
    mo.issue_method_description AS issue_method,

    -- Fiscal year for cost trending
    CASE
        WHEN MONTH(mo.clinical_effective_date) >= 4
        THEN YEAR(mo.clinical_effective_date)
        ELSE YEAR(mo.clinical_effective_date) - 1
    END AS fiscal_year_start

FROM {{ ref('stg_olids_medication_order') }} mo

-- Person mapping
INNER JOIN {{ ref('int_patient_person_unique') }} pp
    ON mo.patient_id = pp.patient_id

-- Prescription details (statement = the prescription, order = each issue)
LEFT JOIN {{ ref('stg_olids_medication_statement') }} ms
    ON mo.medication_statement_id = ms.id

-- Date sanity: source carries ~346k future-dated orders (up to year 2621) and
-- ~58k pre-1990 orders (back to 1799) out of 381M. These poison rolling
-- windows and fiscal_year_start, so exclude them here.
WHERE mo.clinical_effective_date IS NOT NULL
    AND mo.clinical_effective_date >= '1990-01-01'
    AND mo.clinical_effective_date <= CURRENT_DATE
