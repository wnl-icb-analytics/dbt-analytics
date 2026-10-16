{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['polypharmacy']
    )
}}

/*
Latest repeat medication orders that are currently active based on duration.
Filters to repeat prescriptions by joining medication_statement.authorisation_type_code
against the REPEAT_PRESCRIPTION codeset (see CTE below).

Currency logic:
- Takes the most recent order for each person × medication combination
- Considers medication "current" if: order_date + duration_days >= CURRENT_DATE
- Defaults to 28 days duration if NULL (99% of records have duration populated)

Note: Uses medication_order table (not statement) as each order represents when the
GP issues the medication (sends to pharmacy or prints FP10) with accurate duration_days.

Grain: One row per person × medication (latest order only, filtered to current)
*/

WITH repeat_prescription_codes AS (
    -- Get SNOMED codes for repeat prescriptions from cluster
    SELECT DISTINCT code
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE cluster_id = 'REPEAT_PRESCRIPTION'
),

repeat_orders AS (
    -- Join medication orders to statements to filter by authorisation type cluster
    SELECT
        mo.person_id,
        mo.clinical_effective_date AS order_date,
        mo.duration_days AS order_duration_days,
        mo.estimated_cost,
        mo.mapped_concept_code
    FROM {{ ref('stg_olids_medication_order') }} mo
    INNER JOIN {{ ref('stg_olids_medication_statement') }} ms
        ON mo.medication_statement_id = ms.id
    INNER JOIN repeat_prescription_codes rpc
        ON ms.authorisation_type_code = rpc.code
    WHERE mo.clinical_effective_date IS NOT NULL
        AND mo.mapped_concept_code IS NOT NULL
),

latest_orders AS (
    SELECT
        person_id,
        mapped_concept_code,
        MAX(order_date) AS latest_order_date,
        MAX_BY(order_duration_days, order_date) AS latest_duration
    FROM repeat_orders
    GROUP BY person_id, mapped_concept_code
)

SELECT
    person_id,
    mapped_concept_code,
    latest_order_date,
    latest_duration
FROM latest_orders
WHERE DATEADD(day, COALESCE(latest_duration, 28), latest_order_date) >= CURRENT_DATE()
    AND latest_order_date <= CURRENT_DATE()  -- Ignore future orders
