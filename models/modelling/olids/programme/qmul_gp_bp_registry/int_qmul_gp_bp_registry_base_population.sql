{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Base population for the QMUL GP BP Registry research cohort.

Inclusion rules applied here:
  - On the QOF hypertension register (fct_person_hypertension_register.is_on_register = TRUE)
    which encodes the spec's "Adults >= 18" and "diagnosis of hypertension" clauses
    (active diagnosis: latest HYP_COD with no later HYPRES_COD).
  - At least one oral antihypertensive medication order on record.

Further inclusion criteria (BP reading count and spacing) are applied in downstream
models. Exclusion of readings taken during pregnancy/HDP windows is applied separately
to BP events rather than removing patients here.

One row per eligible person.
*/

WITH register AS (

    SELECT
        person_id,
        age,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref('fct_person_hypertension_register') }}
    WHERE is_on_register

),

antihyp_orders AS (

    SELECT
        person_id,
        MIN(order_date) AS earliest_antihyp_order_date,
        MAX(order_date) AS latest_antihyp_order_date
    FROM {{ ref('int_antihypertensive_medications_all') }}
    GROUP BY person_id

)

SELECT
    r.person_id,
    r.age,
    r.earliest_diagnosis_date,
    r.latest_diagnosis_date,
    m.earliest_antihyp_order_date,
    m.latest_antihyp_order_date

FROM register r
INNER JOIN antihyp_orders m ON r.person_id = m.person_id
