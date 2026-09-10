{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Latest antiplatelet and oral anticoagulant orders per person, for "is being
taken" measures such as NICE IND132, IND133 and IND94. One row per person with
either class ever ordered. Antiplatelets are BNF 2.9; oral anticoagulants are
BNF 2.8.2. Orders dated before 1990 or after the build date are ignored.

Consumers apply their own window to the latest dates.
*/

WITH antiplatelet AS (
    SELECT
        person_id,
        MAX(order_date) AS latest_antiplatelet_order_date,
        COUNT(*) AS antiplatelet_order_count
    FROM {{ ref('int_antiplatelet_medications_all') }}
    WHERE order_date BETWEEN '1990-01-01' AND CURRENT_DATE()
    GROUP BY person_id
),

anticoagulant AS (
    SELECT
        person_id,
        MAX(order_date) AS latest_anticoagulant_order_date,
        COUNT(*) AS anticoagulant_order_count,
        MAX_BY(anticoagulant_type, order_date) AS latest_anticoagulant_type,
        MAX(CASE WHEN is_doac THEN order_date END) AS latest_doac_order_date,
        MAX(CASE WHEN is_vka THEN order_date END) AS latest_vka_order_date
    FROM {{ ref('int_anticoagulant_medications_all') }}
    WHERE order_date BETWEEN '1990-01-01' AND CURRENT_DATE()
    GROUP BY person_id
)

SELECT
    COALESCE(ap.person_id, ac.person_id) AS person_id,
    ap.latest_antiplatelet_order_date,
    ap.antiplatelet_order_count,
    ac.latest_anticoagulant_order_date,
    ac.anticoagulant_order_count,
    ac.latest_anticoagulant_type,
    ac.latest_doac_order_date,
    ac.latest_vka_order_date
FROM antiplatelet ap
FULL OUTER JOIN anticoagulant ac ON ap.person_id = ac.person_id
