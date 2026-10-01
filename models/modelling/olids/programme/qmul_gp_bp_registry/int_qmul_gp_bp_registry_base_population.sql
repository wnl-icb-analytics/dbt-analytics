{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Base population for the QMUL GP BP Registry research cohort.

Inclusion rules applied here:
  - Adults aged 18 or over on the QOF hypertension register
    (fct_person_hypertension_register.is_on_register = TRUE)
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
      AND age >= 18

),

antihyp_orders AS (

    SELECT
        person_id,
        MIN(order_date) AS earliest_antihyp_order_date,
        MAX(order_date) AS latest_antihyp_order_date
    FROM {{ ref('int_antihypertensive_medications_all') }}
    -- Exclude non-oral products using dose form in the product name because OLIDS orders carry no route.
    -- Whole words only: a substring such as "gel" matches "Boehringer Ingelheim" tablets.
    WHERE NOT REGEXP_LIKE(
        LOWER(COALESCE(mapped_concept_display, order_medication_name, '')),
        '(.*[^a-z])?(inject[a-z]*|infusion|ampoules?|cream|ointment|gel|patch(es)?|transdermal|eye ?drops?)([^a-z].*)?'
    )
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
