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
checks AS (
    SELECT COUNT(*) = 1 AND COUNT_IF(id = 'SYN_B') = 1 AS passed FROM list_ordering
    UNION ALL
    SELECT COUNT(*) = 1 AND COUNT_IF(clinical_effective_date = '2024-02-01') = 1
    FROM expression_ordering
    UNION ALL
    SELECT COUNT(*) = 1 AND COUNT_IF(id = 'SYN_A') = 1 FROM explicit_ordering
)
SELECT COUNT(*) AS failed_checks
FROM checks
WHERE NOT COALESCE(passed, FALSE)
HAVING COUNT(*) > 0
