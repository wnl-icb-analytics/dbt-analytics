{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All antibacterial medication orders for treatment of bacterial infections.
Uses BNF classification (5.1) for antibacterial drugs.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

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

    -- Antibacterial class classification (based on BNF 5.1 subsections)
    CASE
        WHEN bnf_code LIKE '050101%' THEN 'PENICILLINS'                    -- 5.1.1: Penicillins
        WHEN bnf_code LIKE '050102%' THEN 'CEPHALOSPORINS_BETA_LACTAMS'   -- 5.1.2: Cephalosporins and other beta-lactams
        WHEN bnf_code LIKE '050103%' THEN 'TETRACYCLINES'                 -- 5.1.3: Tetracyclines
        WHEN bnf_code LIKE '050104%' THEN 'AMINOGLYCOSIDES'               -- 5.1.4: Aminoglycosides
        WHEN bnf_code LIKE '050105%' THEN 'MACROLIDES'                    -- 5.1.5: Macrolides
        WHEN bnf_code LIKE '050106%' THEN 'CLINDAMYCIN_LINCOMYCIN'        -- 5.1.6: Clindamycin and lincomycin
        WHEN bnf_code LIKE '050107%' THEN 'OTHER_ANTIBACTERIALS'          -- 5.1.7: Some other antibacterials
        WHEN bnf_code LIKE '050108%' THEN 'SULFONAMIDES_TRIMETHOPRIM'     -- 5.1.8: Sulfonamides and trimethoprim
        WHEN bnf_code LIKE '050109%' THEN 'ANTITUBERCULOSIS'              -- 5.1.9: Antituberculosis drugs
        WHEN bnf_code LIKE '050110%' THEN 'ANTILEPROTIC'                  -- 5.1.10: Antileprotic drugs
        WHEN bnf_code LIKE '050111%' THEN 'METRONIDAZOLE_TINIDAZOLE'      -- 5.1.11: Metronidazole, tinidazole and ornidazole
        WHEN bnf_code LIKE '050112%' THEN 'QUINOLONES'                    -- 5.1.12: Quinolones
        WHEN bnf_code LIKE '050113%' THEN 'URINARY_TRACT_INFECTIONS'     -- 5.1.13: Urinary-tract infections
        ELSE 'UNKNOWN_ANTIBACTERIAL'
    END AS antibacterial_class,

    -- Order recency flags (antibacterials are typically short-term therapy)
    {{ medication_date_flags('order_date') }}

FROM (
    {{ get_medication_orders(bnf_code='0501') }}
) base_orders
ORDER BY person_id, order_date DESC
