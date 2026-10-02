-- depends_on: {{ ref('int_nice_smoking_support_all') }}
-- QOF v51 PHARMDRUG must supply the smoking-support event extractor.
SELECT 12465801000001106 AS missing_ref_set_id
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('stg_nhsd_snomed_sct_refset_simple') }}
    WHERE ref_set_id = 12465801000001106
        AND active
)
