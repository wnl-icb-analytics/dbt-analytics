SELECT indicator_id
FROM {{ ref('indicator_classification') }}
WHERE NOT (
    (NULLIF(TRIM(condition_code), '') IS NOT NULL
        AND NULLIF(TRIM(clinical_domain), '') IS NULL
        AND NULLIF(TRIM(clinical_subdomain), '') IS NULL)
    OR
    (NULLIF(TRIM(condition_code), '') IS NULL
        AND NULLIF(TRIM(clinical_domain), '') IS NOT NULL
        AND NULLIF(TRIM(clinical_subdomain), '') IS NOT NULL)
)
