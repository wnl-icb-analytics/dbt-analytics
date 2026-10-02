{{ config(materialized='table', cluster_by=['person_id']) }}

-- NICE IND207 categories, one contribution per person and category on the build date.
WITH register_categories AS (
    SELECT person_id,
        CASE
            WHEN condition_code = 'CAN' THEN 'CANCER'
            WHEN condition_code IN ('CHD', 'AF', 'HF', 'HTN', 'STIA', 'PAD') THEN 'CIRCULATORY'
            WHEN condition_code = 'DM' THEN 'DIABETES'
            WHEN condition_code = 'CLD' THEN 'DIGESTIVE'
            WHEN condition_code IN ('LD', 'LD_U14') THEN 'LEARNING_DISABILITY'
            WHEN condition_code IN ('ANX', 'DEP', 'DEM')
                OR (condition_code = 'SMI' AND earliest_diagnosis_date IS NOT NULL) THEN 'MENTAL_HEALTH'
            WHEN condition_code = 'RA' THEN 'MUSCULOSKELETAL'
            WHEN condition_code IN ('EP', 'MS', 'PD') THEN 'NEUROLOGICAL'
            WHEN condition_code = 'CKD' THEN 'RENAL'
            WHEN condition_code IN ('AST', 'CYP_AST', 'COPD') THEN 'RESPIRATORY'
        END AS category_code
    FROM {{ ref('fct_person_ltc_summary') }}
    WHERE is_on_register

    UNION ALL

    SELECT person_id, 'MENTAL_HEALTH'
    FROM {{ ref('int_ltc_review_profile') }}
    WHERE has_alcohol_disorder
),

pcd_conditions AS (
    SELECT person_id,
        IFF(cluster_id = 'EATDISORDER_COD', 'MENTAL_HEALTH', 'DIGESTIVE') AS category_code
    FROM ({{ get_observations("'CROHNS_COD', 'ULCCOLITIS_COD', 'EATDISORDER_COD'", source='PCD') }})
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()
        -- NICE names anorexia or bulimia, not all eating disorders or care/history codes.
        AND (cluster_id <> 'EATDISORDER_COD'
            OR (code_description ILIKE '%(disorder)%'
                AND (code_description ILIKE '%anorexia nervosa%' OR code_description ILIKE '%bulimia%')))
    GROUP BY person_id, category_code
),

other_conditions AS (
    SELECT person_id,
        CASE
            WHEN cluster_id = 'QCOVID/HAS_CF_OR_BRONCHIECTASIS' THEN 'RESPIRATORY'
            WHEN cluster_id = 'OPENSAFELY/INFLAMMATORY_BOWEL_DISEASE_UNCLASSIFIED' THEN 'DIGESTIVE'
            ELSE 'MUSCULOSKELETAL'
        END AS category_code
    FROM ({{ get_observations("'BRISTOL/MULTIMORBIDITY_CONNECTIVE_TISSUE_DISORDER', 'QCOVID/HAS_CF_OR_BRONCHIECTASIS', 'OPENSAFELY/INFLAMMATORY_BOWEL_DISEASE_UNCLASSIFIED'", source='OPENCODELISTS') }})
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()
        -- The QCovid set also contains cystic fibrosis and associated syndromes.
        AND (cluster_id <> 'QCOVID/HAS_CF_OR_BRONCHIECTASIS'
            OR code_description ILIKE '%bronchiectasis%')
    GROUP BY person_id, category_code
),

-- Any diagnosis known by today excludes the antiepileptic pain route, including resolved epilepsy.
epilepsy_diagnoses AS (
    SELECT person_id
    FROM {{ ref('int_epilepsy_diagnoses_all') }}
    WHERE is_diagnosis_code
        AND clinical_effective_date::DATE <= CURRENT_DATE()
    GROUP BY person_id
),

prescription_counts AS (
    -- Cambridge dm+d sets: analgesics excluding migraine drugs, antiepileptics
    -- excluding benzodiazepines, and laxatives. Count issues, not cluster matches.
    SELECT person_id,
        COUNT(DISTINCT IFF(conditionid = 5065, medication_order_id, NULL)) AS analgesic_issues,
        COUNT(DISTINCT IFF(conditionid = 5066, medication_order_id, NULL)) AS antiepileptic_issues,
        COUNT(DISTINCT IFF(conditionid = 5071, medication_order_id, NULL)) AS laxative_issues
    FROM {{ ref('int_ccms_medication_orders') }}
    WHERE conditionid IN (5065, 5066, 5071)
        AND order_date BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
    GROUP BY person_id
),

additional_categories AS (
    SELECT person_id, category_code FROM pcd_conditions
    UNION ALL
    SELECT person_id, category_code FROM other_conditions
    UNION ALL
    SELECT person_id, 'MENTAL_HEALTH'
    FROM {{ ref('int_substance_misuse_status') }}
    WHERE has_substance_misuse
    UNION ALL
    SELECT prescribing.person_id, 'CHRONIC_PAIN'
    FROM prescription_counts AS prescribing
    LEFT JOIN epilepsy_diagnoses AS epilepsy ON prescribing.person_id = epilepsy.person_id
    WHERE prescribing.analgesic_issues >= 4
        OR (prescribing.antiepileptic_issues >= 4 AND epilepsy.person_id IS NULL)
    UNION ALL
    SELECT person_id, 'DIGESTIVE'
    FROM prescription_counts
    WHERE laxative_issues >= 4
),

category_evidence AS (
    SELECT person_id, category_code, TRUE AS has_existing_evidence, FALSE AS has_additional_evidence
    FROM register_categories
    WHERE category_code IS NOT NULL
    UNION ALL
    SELECT person_id, category_code, FALSE, TRUE
    FROM additional_categories
)

SELECT person_id, category_code,
    BOOLOR_AGG(has_existing_evidence) AS has_existing_evidence,
    BOOLOR_AGG(has_additional_evidence) AS has_additional_evidence
FROM category_evidence
WHERE person_id IS NOT NULL
GROUP BY person_id, category_code
