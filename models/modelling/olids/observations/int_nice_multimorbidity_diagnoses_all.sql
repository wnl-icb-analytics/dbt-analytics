{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

-- Additional NICE IND207 diagnoses, one row per observation and source cluster.
WITH diagnoses AS (
    SELECT
        id,
        person_id,
        clinical_effective_date,
        date_recorded,
        cluster_id,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display,
        IFF(cluster_id = 'EATDISORDER_COD', 'MENTAL_HEALTH', 'DIGESTIVE') AS category_code
    FROM ({{ get_observations("'CROHNS_COD', 'ULCCOLITIS_COD', 'EATDISORDER_COD'", source='PCD') }})
    -- NICE specifies anorexia or bulimia disorder concepts, not care/history codes.
    WHERE cluster_id <> 'EATDISORDER_COD'
        OR (code_description ILIKE '%(disorder)%'
            AND (code_description ILIKE '%anorexia nervosa%'
                OR code_description ILIKE '%bulimia%'))

    UNION ALL

    SELECT
        id,
        person_id,
        clinical_effective_date,
        date_recorded,
        cluster_id,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display,
        CASE
            WHEN cluster_id = 'QCOVID/HAS_CF_OR_BRONCHIECTASIS' THEN 'RESPIRATORY'
            WHEN cluster_id = 'OPENSAFELY/INFLAMMATORY_BOWEL_DISEASE_UNCLASSIFIED' THEN 'DIGESTIVE'
            ELSE 'MUSCULOSKELETAL'
        END AS category_code
    FROM ({{ get_observations("'BRISTOL/MULTIMORBIDITY_CONNECTIVE_TISSUE_DISORDER', 'QCOVID/HAS_CF_OR_BRONCHIECTASIS', 'OPENSAFELY/INFLAMMATORY_BOWEL_DISEASE_UNCLASSIFIED'", source='OPENCODELISTS') }})
    -- The combined QCovid set also contains cystic fibrosis and associated syndromes.
    WHERE cluster_id <> 'QCOVID/HAS_CF_OR_BRONCHIECTASIS'
        OR code_description ILIKE '%bronchiectasis%'
)
SELECT
    id,
    person_id,
    clinical_effective_date,
    date_recorded,
    cluster_id,
    concept_code,
    concept_display,
    category_code
FROM diagnoses
WHERE clinical_effective_date::DATE <= CURRENT_DATE()
