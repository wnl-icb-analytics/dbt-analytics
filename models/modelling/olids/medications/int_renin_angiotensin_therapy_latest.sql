{{ config(cluster_by=['person_id']) }}

/*
Latest renin-angiotensin system therapy order per person, for "currently
treated" measures: ACE inhibitors (BNF 2.5.5.1) and angiotensin II receptor
blockers (BNF 2.5.5.2). Consumers apply their own window to latest_order_date;
the NICE indicators use six calendar months before the reporting date.
Orders before 1990 or after the build date are ignored.
*/

WITH orders AS (
    SELECT person_id, medication_order_id, order_date, bnf_name, 'ACE_INHIBITOR' AS ras_class
    FROM {{ ref('int_ace_inhibitor_medications_all') }}
    WHERE order_date BETWEEN '1990-01-01' AND CURRENT_DATE()

    UNION ALL

    SELECT person_id, medication_order_id, order_date, bnf_name, 'ARB' AS ras_class
    FROM {{ ref('int_arb_medications_all') }}
    WHERE order_date BETWEEN '1990-01-01' AND CURRENT_DATE()
),

per_person AS (
    SELECT
        person_id,
        MIN(order_date) AS first_order_date,
        MAX(CASE WHEN ras_class = 'ACE_INHIBITOR' THEN order_date END) AS latest_ace_inhibitor_order_date,
        MAX(CASE WHEN ras_class = 'ARB' THEN order_date END) AS latest_arb_order_date,
        COUNT(*) AS order_count
    FROM orders
    GROUP BY person_id
),

latest_order AS (
    SELECT *
    FROM orders
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id ORDER BY order_date DESC, medication_order_id DESC
    ) = 1
)

SELECT
    latest.person_id,
    latest.order_date AS latest_order_date,
    latest.medication_order_id AS latest_order_id,
    latest.ras_class AS latest_ras_class,
    latest.bnf_name AS latest_product_name,
    per_person.latest_ace_inhibitor_order_date,
    per_person.latest_arb_order_date,
    per_person.first_order_date,
    per_person.order_count
FROM latest_order AS latest
INNER JOIN per_person ON latest.person_id = per_person.person_id
