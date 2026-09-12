-- Every ILLSUB_COD code published to COMBINED_CODESETS must carry a
-- QUALIFYING/RESOLVING classification in the substance_misuse_illsub_status seed.
--
-- int_substance_misuse_status defaults unclassified codes to QUALIFYING so that a
-- refset update cannot silently drop people out of the cohort. This test makes the
-- other side of that trade loud: if NHSE adds codes to ILLSUB_COD, it fails and the
-- seed is re-cut from the refset rather than the default quietly deciding who
-- counts as a current substance misuser.

SELECT
    cs.code,
    cs.code_description
FROM {{ ref('stg_reference_combined_codesets') }} AS cs
LEFT JOIN {{ ref('substance_misuse_illsub_status') }} AS s
    ON cs.code = s.snomed_code
WHERE UPPER(cs.cluster_id) = 'ILLSUB_COD'
    AND s.snomed_code IS NULL
