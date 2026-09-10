-- Fails when an observed NDAALB_COD code lands in OTHER without being on the
-- documented exclusion list in int_urine_acr_all, so cluster additions are
-- classified rather than silently dropped from every ACR consumer.
SELECT
    concept_code,
    MIN(concept_display) AS concept_display,
    COUNT(*) AS observation_count
FROM {{ ref('int_urine_acr_all') }}
WHERE albumin_test_type = 'OTHER'
    AND concept_code NOT IN ('252242007', '30711000237105')
GROUP BY concept_code
