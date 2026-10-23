{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All diabetes medication orders including insulins, antidiabetic drugs, and hypoglycaemia treatments.
Uses BNF classification (6.1.x) with detailed medication type categorisation.
GLP-1 receptor agonists, SGLT2 inhibitors and DPP-4 inhibitors are broken out of BNF
6.1.2.3 (Other antidiabetic drugs) using int_glp1_medications_all /
int_sglt2_medications_all / int_dpp4_medications_all, which categorise them to active
ingredient via SNOMED clusters.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH base_orders AS (
    {{ get_medication_orders(bnf_code='0601') }}
),

-- GLP-1 RA classification by active ingredient, joined back on order id
glp1 AS (
    SELECT
        medication_order_id,
        glp1_drug,
        is_dual_gip_glp1
    FROM {{ ref('int_glp1_medications_all') }}
),

-- SGLT2 inhibitor classification by active ingredient, joined back on order id
sglt2 AS (
    SELECT
        medication_order_id,
        sglt2_drug
    FROM {{ ref('int_sglt2_medications_all') }}
),

-- DPP-4 inhibitor classification by active ingredient, joined back on order id
dpp4 AS (
    SELECT
        medication_order_id,
        dpp4_drug
    FROM {{ ref('int_dpp4_medications_all') }}
),

-- Metformin (incl. combinations) by cluster membership, joined back on order id
metformin AS (
    SELECT
        medication_order_id
    FROM {{ ref('int_metformin_medications_all') }}
)

SELECT
    bo.person_id,
    bo.medication_order_id,
    bo.medication_statement_id,
    bo.order_date,
    bo.order_medication_name,
    bo.order_dose,
    bo.order_quantity_value,
    bo.order_quantity_unit,
    bo.order_duration_days,
    bo.statement_medication_name,
    bo.mapped_concept_code,
    bo.mapped_concept_display,
    bo.bnf_code,
    bo.bnf_name,

    -- Diabetes medication type classification (corrected BNF codes)
    CASE
        WHEN bo.bnf_code LIKE '0601011%' OR bo.bnf_code LIKE '0601012%' THEN 'INSULIN'  -- BNF 6.1.1: Insulins
        WHEN bo.bnf_code LIKE '0601021%' OR bo.bnf_code LIKE '0601022%' OR bo.bnf_code LIKE '0601023%' THEN 'ANTIDIABETIC'  -- BNF 6.1.2: Antidiabetic drugs
        WHEN bo.bnf_code LIKE '0601040%' THEN 'HYPOGLYCAEMIA_TREATMENT'   -- BNF 6.1.4: Treatment of hypoglycaemia
        WHEN bo.bnf_code LIKE '0601060%' THEN 'MONITORING'                -- BNF 6.1.6: Diabetic diagnostic and monitoring agents
        ELSE 'OTHER_DIABETES'
    END AS diabetes_medication_type,

    -- Antidiabetic drug class classification (BNF 6.1.2 subcodes).
    -- GLP-1 RAs, SGLT2 inhibitors and DPP-4 inhibitors are flagged from SNOMED clusters (all sit within 6.1.2.3 by BNF).
    CASE
        WHEN g.medication_order_id IS NOT NULL THEN 'GLP1_RECEPTOR_AGONIST'
        WHEN s.medication_order_id IS NOT NULL THEN 'SGLT2_INHIBITOR'
        WHEN d.medication_order_id IS NOT NULL THEN 'DPP4_INHIBITOR'
        WHEN bo.bnf_code LIKE '0601021%' THEN 'SULPHONYLUREAS'            -- 6.1.2.1: Sulphonylureas
        WHEN bo.bnf_code LIKE '0601022%' THEN 'BIGUANIDES'                -- 6.1.2.2: Biguanides (metformin)
        WHEN bo.bnf_code LIKE '0601023%' THEN 'OTHER_ANTIDIABETICS'       -- 6.1.2.3: Other antidiabetic drugs
        ELSE NULL
    END AS antidiabetic_drug_class,

    -- GLP-1 RA / SGLT2 / DPP-4 active ingredient (null for orders not in that class)
    g.glp1_drug,
    s.sglt2_drug,
    d.dpp4_drug,

    -- Key medication flags (corrected BNF codes)
    CASE WHEN bo.bnf_code LIKE '0601011%' OR bo.bnf_code LIKE '0601012%' THEN TRUE ELSE FALSE END AS is_insulin,
    -- Metformin flag from the SNOMED cluster so it also catches combination products (BNF codes these under 6.1.2.3)
    CASE WHEN m.medication_order_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_metformin,
    CASE WHEN bo.bnf_code LIKE '0601021%' THEN TRUE ELSE FALSE END AS is_sulphonylurea,
    CASE WHEN g.medication_order_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_glp1_ra,
    COALESCE(g.is_dual_gip_glp1, FALSE) AS is_dual_gip_glp1,
    CASE WHEN s.medication_order_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_sglt2_inhibitor,
    CASE WHEN d.medication_order_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_dpp4_inhibitor,


    -- Order recency flags
    {{ medication_date_flags('bo.order_date', months=[3, 6]) }}

FROM base_orders bo
LEFT JOIN glp1 g
    ON bo.medication_order_id = g.medication_order_id
LEFT JOIN sglt2 s
    ON bo.medication_order_id = s.medication_order_id
LEFT JOIN dpp4 d
    ON bo.medication_order_id = d.medication_order_id
LEFT JOIN metformin m
    ON bo.medication_order_id = m.medication_order_id
ORDER BY bo.person_id, bo.order_date DESC
