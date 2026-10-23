{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'allergy', 'antihistamines'])
}}

/*
All allergy medication orders including antihistamines and allergy treatments.
Uses BNF classification (3.4) for antihistamines and related allergy medications.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.

This is OBSERVATION-LEVEL data - one row per medication order.
Person-level aggregation should be handled in downstream fact models.
*/

SELECT
    bo.*,

    -- Allergy medication type classification (based on BNF subcategories)
    CASE
        WHEN bo.bnf_code LIKE '030401%' THEN 'ANTIHISTAMINES'           -- 3.4.1: Antihistamines
        WHEN bo.bnf_code LIKE '030402%' THEN 'ALLERGEN_IMMUNOTHERAPY'   -- 3.4.2: Allergen immunotherapy
        WHEN bo.bnf_code LIKE '030403%' THEN 'ALLERGIC_EMERGENCIES'     -- 3.4.3: Allergic emergencies
        ELSE 'OTHER_ALLERGY_TREATMENT'
    END AS allergy_medication_type,

    -- Allergy medication class flags
    CASE WHEN bo.bnf_code LIKE '030401%' THEN TRUE ELSE FALSE END AS is_antihistamine,
    CASE WHEN bo.bnf_code LIKE '030402%' THEN TRUE ELSE FALSE END AS is_allergen_immunotherapy,
    CASE WHEN bo.bnf_code LIKE '030403%' THEN TRUE ELSE FALSE END AS is_anaphylaxis_treatment,

    -- Recency flags for monitoring (order-level only)
    {{ medication_date_flags('bo.order_date') }}

FROM ({{ get_medication_orders(bnf_code='0304') }}) bo

-- Order by person and date for analysis
ORDER BY bo.person_id, bo.order_date DESC
