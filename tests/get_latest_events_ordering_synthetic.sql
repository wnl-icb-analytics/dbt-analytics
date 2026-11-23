-- Caller models do not exercise every order expression and NULLS override supported by this macro.
WITH synthetic_events AS (
    SELECT -18006::NUMBER AS person_id, column1::VARCHAR AS id,
        column2::DATE AS clinical_effective_date
    FROM VALUES ('SYN_A', '2024-01-01'), ('SYN_Z', '2024-02-01'), ('SYN_B', '2024-02-01')
),
list_ordering AS (
    {{ get_latest_events('synthetic_events', order_by=['clinical_effective_date', 'id ASC']) }}
),
expression_ordering AS (
    {{ get_latest_events('synthetic_events', order_by="coalesce(clinical_effective_date, '1900-01-01')") }}
),
explicit_ordering AS (
    {{ get_latest_events('synthetic_events', order_by='clinical_effective_date asc nulls last') }}
),
nullable_events AS (
    SELECT * FROM synthetic_events
    UNION ALL
    SELECT -18016::NUMBER, column1::VARCHAR, column2::DATE
    FROM VALUES ('SYN_OLD', '2024-01-01'), ('SYN_NEW', '2024-02-01'), ('SYN_NULL', NULL)
),
nulls_last_ordering AS (
    {{ get_latest_events('nullable_events', order_by=['clinical_effective_date NULLS LAST', 'id ASC']) }}
),
nulls_first_ordering AS (
    {{ get_latest_events('nullable_events', order_by=['clinical_effective_date nulls first', 'id ASC']) }}
),
asc_nulls_last_ordering AS (
    {{ get_latest_events('nullable_events', order_by='clinical_effective_date ASC NULLS LAST') }}
),
asc_nulls_first_ordering AS (
    {{ get_latest_events('nullable_events', order_by='clinical_effective_date asc nulls first') }}
),
desc_nulls_last_ordering AS (
    {{ get_latest_events('nullable_events', order_by='clinical_effective_date desc nulls last', direction='ASC') }}
),
desc_nulls_first_ordering AS (
    {{ get_latest_events('nullable_events', order_by='clinical_effective_date DESC NULLS FIRST', direction='ASC') }}
),
actual AS (
    SELECT 'list_ordering' AS check_name, * FROM list_ordering
    UNION ALL
    SELECT 'expression_ordering', * FROM expression_ordering
    UNION ALL
    SELECT 'explicit_ordering', * FROM explicit_ordering
    UNION ALL
    SELECT 'nulls_last_ordering', * FROM nulls_last_ordering
    UNION ALL
    SELECT 'nulls_first_ordering', * FROM nulls_first_ordering
    UNION ALL
    SELECT 'asc_nulls_last_ordering', * FROM asc_nulls_last_ordering
    UNION ALL
    SELECT 'asc_nulls_first_ordering', * FROM asc_nulls_first_ordering
    UNION ALL
    SELECT 'desc_nulls_last_ordering', * FROM desc_nulls_last_ordering
    UNION ALL
    SELECT 'desc_nulls_first_ordering', * FROM desc_nulls_first_ordering
),
actual_rows AS (
    SELECT *, COUNT(*) OVER (PARTITION BY check_name, person_id) AS row_count
    FROM actual
),
expected AS (
    -- A null expected ID allows either tied event when only the date is ordered.
    SELECT column1::VARCHAR AS check_name, column2::NUMBER AS person_id,
        column3::VARCHAR AS id, column4::DATE AS clinical_effective_date
    FROM VALUES
        ('list_ordering', -18006, 'SYN_B', '2024-02-01'),
        ('expression_ordering', -18006, NULL, '2024-02-01'),
        ('explicit_ordering', -18006, 'SYN_A', '2024-01-01'),
        ('nulls_last_ordering', -18006, 'SYN_B', '2024-02-01'),
        ('nulls_last_ordering', -18016, 'SYN_NEW', '2024-02-01'),
        ('nulls_first_ordering', -18006, 'SYN_B', '2024-02-01'),
        ('nulls_first_ordering', -18016, 'SYN_NULL', NULL),
        ('asc_nulls_last_ordering', -18006, 'SYN_A', '2024-01-01'),
        ('asc_nulls_last_ordering', -18016, 'SYN_OLD', '2024-01-01'),
        ('asc_nulls_first_ordering', -18006, 'SYN_A', '2024-01-01'),
        ('asc_nulls_first_ordering', -18016, 'SYN_NULL', NULL),
        ('desc_nulls_last_ordering', -18006, NULL, '2024-02-01'),
        ('desc_nulls_last_ordering', -18016, 'SYN_NEW', '2024-02-01'),
        ('desc_nulls_first_ordering', -18006, NULL, '2024-02-01'),
        ('desc_nulls_first_ordering', -18016, 'SYN_NULL', NULL)
)
SELECT
    COALESCE(actual.check_name, expected.check_name) AS check_name,
    COALESCE(actual.person_id, expected.person_id) AS person_id,
    actual.id AS actual_id, expected.id AS expected_id,
    actual.clinical_effective_date AS actual_date,
    expected.clinical_effective_date AS expected_date,
    actual.row_count AS actual_row_count, 1 AS expected_row_count
FROM actual_rows AS actual
FULL OUTER JOIN expected USING (check_name, person_id)
WHERE actual.check_name IS NULL OR expected.check_name IS NULL
    OR (expected.id IS NOT NULL AND NOT EQUAL_NULL(actual.id, expected.id))
    OR NOT EQUAL_NULL(actual.clinical_effective_date, expected.clinical_effective_date)
    OR actual.row_count <> 1
