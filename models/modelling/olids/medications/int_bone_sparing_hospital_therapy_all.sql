{{ config(materialized='table', cluster_by=['person_id', 'therapy_date']) }}

WITH observations AS (
    SELECT person_id, id::VARCHAR AS record_id, clinical_effective_date::DATE AS therapy_date,
        'PCD_OBSERVATION' AS therapy_source, mapped_concept_code, mapped_concept_display,
        CASE
            WHEN mapped_concept_display ILIKE '%zoledronic%' THEN 'ZOLEDRONIC_ACID'
            WHEN mapped_concept_display ILIKE '%intravenous%bisphosphonate%' THEN 'INTRAVENOUS_BISPHOSPHONATE'
            WHEN mapped_concept_display ILIKE '%denosumab%' THEN 'DENOSUMAB'
            ELSE 'OTHER_BONE_SPARING_THERAPY'
        END AS therapy_type
    FROM ({{ get_observations("'BSA_COD'", source='PCD') }})
    WHERE clinical_effective_date IS NOT NULL
), statements AS (
    SELECT person_id, medication_statement_id::VARCHAR AS record_id, statement_date::DATE AS therapy_date,
        'MEDICATION_STATEMENT' AS therapy_source, mapped_concept_code, mapped_concept_display,
        CASE
            -- BNF ingredient prefixes also identify branded products such as Aclasta and Prolia.
            WHEN bnf_code LIKE '0606020V0%' OR mapped_concept_display ILIKE '%zoledronic%'
                THEN 'ZOLEDRONIC_ACID'
            ELSE 'DENOSUMAB'
        END AS therapy_type
    FROM ({{ get_medication_statements(cluster_id='NHS_DRUG_REFSETS/BSADRUG_COD', source='OPENCODELISTS') }})
    WHERE bnf_code LIKE '0606020V0%' OR bnf_code LIKE '0606020Z0%'
        OR mapped_concept_display ILIKE '%zoledronic%' OR mapped_concept_display ILIKE '%denosumab%'
), therapy AS (
    SELECT * FROM observations
    UNION ALL
    SELECT * FROM statements
)
SELECT person_id, record_id, therapy_date, therapy_source, therapy_type,
    IFF(therapy_type IN ('ZOLEDRONIC_ACID', 'INTRAVENOUS_BISPHOSPHONATE'), 12, 6) AS coverage_months,
    mapped_concept_code, mapped_concept_display
FROM therapy
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY therapy_source, record_id ORDER BY mapped_concept_code DESC
) = 1
