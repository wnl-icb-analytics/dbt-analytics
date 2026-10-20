{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All oral anticoagulant medication orders for thrombosis prevention and atrial fibrillation management.
Uses BNF classification (2.8.2) for oral anticoagulants.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH national_membership AS (
    SELECT code,
        BOOLOR_AGG(cluster_id = 'NHS_DRUG_REFSETS/DIRECTORANTICOAGDRUG_COD') AS is_national_doac,
        BOOLOR_AGG(cluster_id = 'NHS_DRUG_REFSETS/VITKANTAGDRUG_COD') AS is_national_vka
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE source = 'OPENCODELISTS'
        AND cluster_id IN ('NHS_DRUG_REFSETS/DIRECTORANTICOAGDRUG_COD',
            'NHS_DRUG_REFSETS/VITKANTAGDRUG_COD')
    GROUP BY code
)

SELECT
    person_id,
    medication_order_id,
    medication_statement_id,
    order_date,
    order_medication_name,
    order_dose,
    order_quantity_value,
    order_quantity_unit,
    order_duration_days,
    statement_medication_name,
    mapped_concept_code,
    mapped_concept_display,
    bnf_code,
    bnf_name,

    -- Anticoagulant type classification (based on BNF codes)
    CASE
        -- DOACs (Direct Oral Anticoagulants)
        WHEN bnf_code LIKE '0208020Z%' THEN 'DOAC'  -- APIXABAN
        WHEN bnf_code LIKE '0208020X%' THEN 'DOAC'  -- DABIGATRAN
        WHEN bnf_code LIKE '0208020AA%' THEN 'DOAC' -- EDOXABAN
        WHEN bnf_code LIKE '0208020Y%' THEN 'DOAC'  -- RIVAROXABAN
        -- VKAs (Vitamin K Antagonists)
        WHEN bnf_code LIKE '0208020V%' THEN 'VKA'   -- WARFARIN
        WHEN bnf_code LIKE '0208020H%' THEN 'VKA'   -- ACENOCOUMAROL
        WHEN bnf_code LIKE '0208020N%' THEN 'VKA'   -- PHENINDIONE
        WHEN bnf_code LIKE '0208020S%' THEN 'VKA'   -- PHENPROCOUMON
        ELSE 'OTHER_ANTICOAGULANT'
    END AS anticoagulant_type,

    -- Anticoagulant class flags
    CASE
        WHEN bnf_code LIKE '0208020Z%' OR bnf_code LIKE '0208020X%'
             OR bnf_code LIKE '0208020Y%' OR bnf_code LIKE '0208020AA%' THEN TRUE
        ELSE FALSE
    END AS is_doac,

    CASE
        WHEN bnf_code LIKE '0208020V%' OR bnf_code LIKE '0208020H%'
             OR bnf_code LIKE '0208020N%' OR bnf_code LIKE '0208020S%' THEN TRUE
        ELSE FALSE
    END AS is_vka,

    -- Order recency flags (anticoagulants are typically long-term therapy)
    {{ medication_date_flags('order_date') }},
    -- Legacy membership stays until the semantic category removal is approved.
    coalesce(lm.is_national_doac OR lm.is_national_vka, false) AS is_in_national_anticoagulant_list,
    CASE WHEN lm.is_national_doac THEN 'DOAC'
         WHEN lm.is_national_vka THEN 'VKA' END AS anticoagulant_type_national

FROM (
    {{ get_medication_orders(bnf_code='020802') }}
) base_orders
LEFT JOIN national_membership lm ON base_orders.mapped_concept_code = lm.code
ORDER BY person_id, order_date DESC
