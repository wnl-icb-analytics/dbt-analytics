{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Latest lipid-lowering therapy order per person, for "currently treated" measures.

Counted classes are statins (alone or in combination), ezetimibe, bempedoic acid,
PCSK9 inhibitors, inclisiran, fibrates and bile acid sequestrants: products whose
purpose is to lower LDL or non-HDL cholesterol, matching the NICE indicator wording
"a statin or non-statin lipid-lowering therapy". Nicotinic acid and omega-3 products
mainly act on triglycerides and are not counted, nor are other section 2.12 items
such as ispaghula husk. NG238 does not recommend fibrates or sequestrants for CVD
prevention, but a person prescribed one is still on lipid-lowering therapy.

Consumers apply their own window to latest_order_date. The NICE indicators use six
calendar months before the reporting date; the build-time is_recent_6m flag on the
orders model is a 180-day convenience and is not used here.
*/

WITH counted_orders AS (
    SELECT
        person_id,
        medication_order_id,
        order_date,
        lipid_lowering_class,
        bnf_name,
        is_statin,
        statin_intensity
    FROM {{ ref('int_lipid_lowering_medications_all') }}
    WHERE lipid_lowering_class IN (
        'STATIN',
        'STATIN_COMBINATION',
        'EZETIMIBE',
        'BEMPEDOIC_ACID',
        'PCSK9_INHIBITOR',
        'INCLISIRAN',
        'FIBRATE',
        'BILE_ACID_SEQUESTRANT'
    )
),

per_person AS (
    SELECT
        person_id,
        MIN(order_date) AS first_order_date,
        MAX(CASE WHEN is_statin THEN order_date END) AS latest_statin_order_date,
        COUNT(*) AS order_count
    FROM counted_orders
    GROUP BY person_id
),

latest_order AS (
    SELECT *
    FROM counted_orders
    -- Same-day ties: statin first, then the higher order id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY order_date DESC, is_statin DESC, medication_order_id DESC
    ) = 1
)

SELECT
    latest.person_id,
    latest.order_date AS latest_order_date,
    latest.medication_order_id AS latest_order_id,
    latest.lipid_lowering_class AS latest_lipid_lowering_class,
    latest.bnf_name AS latest_product_name,
    latest.is_statin AS is_latest_order_statin,
    latest.statin_intensity AS latest_statin_intensity,
    per_person.latest_statin_order_date,
    per_person.first_order_date,
    per_person.order_count
FROM latest_order latest
INNER JOIN per_person ON latest.person_id = per_person.person_id
