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
checks AS (
    SELECT COUNT(*) = 1 AND COUNT_IF(id = 'SYN_B') = 1 AS passed FROM list_ordering
    UNION ALL
    SELECT COUNT(*) = 1 AND COUNT_IF(clinical_effective_date = '2024-02-01') = 1
    FROM expression_ordering
    UNION ALL
    SELECT COUNT(*) = 1 AND COUNT_IF(id = 'SYN_A') = 1 FROM explicit_ordering
    UNION ALL
    SELECT COUNT(*) = 2 AND COUNT_IF(clinical_effective_date = '2024-02-01') = 2
    FROM nulls_last_ordering
    UNION ALL
    SELECT COUNT(*) = 2 AND COUNT_IF(clinical_effective_date = '2024-02-01') = 1
        AND COUNT_IF(id = 'SYN_NULL') = 1 FROM nulls_first_ordering
    UNION ALL
    SELECT COUNT(*) = 2 AND COUNT_IF(clinical_effective_date = '2024-01-01') = 2
    FROM asc_nulls_last_ordering
    UNION ALL
    SELECT COUNT(*) = 2 AND COUNT_IF(clinical_effective_date = '2024-01-01') = 1
        AND COUNT_IF(id = 'SYN_NULL') = 1 FROM asc_nulls_first_ordering
    UNION ALL
    SELECT COUNT(*) = 2 AND COUNT_IF(clinical_effective_date = '2024-02-01') = 2
    FROM desc_nulls_last_ordering
    UNION ALL
    SELECT COUNT(*) = 2 AND COUNT_IF(clinical_effective_date = '2024-02-01') = 1
        AND COUNT_IF(id = 'SYN_NULL') = 1 FROM desc_nulls_first_ordering
)
SELECT COUNT(*) AS failed_checks
FROM checks
WHERE NOT COALESCE(passed, FALSE)
HAVING COUNT(*) > 0
