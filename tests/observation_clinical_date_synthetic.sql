-- Synthetic null-date control for callers that do not use the observation sentinel.
-- Current caller models replace null dates, so test the macro's null result directly.
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
SELECT
    'missing_clinical_date_stays_null' AS check_name,
    actual.id AS actual_id, expected.id AS expected_id,
    actual.clinical_effective_date AS actual_date,
    expected.clinical_effective_date AS expected_date
FROM actual
FULL OUTER JOIN expected USING (id)
WHERE actual.id IS NULL OR expected.id IS NULL
    OR NOT EQUAL_NULL(actual.clinical_effective_date, expected.clinical_effective_date)
