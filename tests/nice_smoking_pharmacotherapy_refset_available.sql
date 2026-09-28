-- depends_on: {{ ref('int_ltc_review_profile') }}
-- QOF v51 PHARMDRUG_COD must supply drug evidence for NICE smoking support.
SELECT 12465801000001106 AS missing_ref_set_id
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('stg_nhsd_snomed_sct_refset_simple') }}
    WHERE ref_set_id = 12465801000001106 AND active
)
