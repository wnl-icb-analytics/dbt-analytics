-- Synthetic null-date control for callers that do not use the observation sentinel.
WITH synthetic_observation AS (
    SELECT 'SYN_NULL'::VARCHAR AS id, NULL::TIMESTAMP_NTZ AS clinical_effective_date,
        '2026-01-01'::TIMESTAMP_NTZ AS date_recorded
),
actual AS (
    SELECT id, {{ observation_clinical_date('o') }} AS clinical_effective_date
    FROM synthetic_observation AS o
),
expected AS (
    SELECT 'SYN_NULL'::VARCHAR AS id, NULL::TIMESTAMP_NTZ AS clinical_effective_date
)
SELECT COUNT(*) AS mismatches
FROM actual
FULL OUTER JOIN expected USING (id)
WHERE actual.id IS NULL OR expected.id IS NULL
    OR NOT EQUAL_NULL(actual.clinical_effective_date, expected.clinical_effective_date)
HAVING COUNT(*) > 0
