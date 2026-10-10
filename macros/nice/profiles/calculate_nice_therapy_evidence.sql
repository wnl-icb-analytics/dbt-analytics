{% macro calculate_nice_therapy_evidence(reference='current') %}
{#-
    Select each therapy class independently through the reference date.
    Args: reference is current or by_month.
    Returns: person_id, reporting_date, latest LLT/statin/antiplatelet/oral
             anticoagulant/DOAC/VKA/RAS/SGLT2 dates and selected-product fields,
             first LLT date and latest RAS date on/before the selected SGLT2 order.
-#}
-- NICE therapy evidence for LTC members or people aged 25 to 84.
WITH ltc_people AS (
    SELECT DISTINCT
        person_id,
        reporting_date
    FROM ({{ nice_ltc_summary(reference) }})
),

population AS (
    SELECT
        population.person_id,
        population.reporting_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    LEFT JOIN ltc_people AS ltc
        ON population.person_id = ltc.person_id
        AND population.reporting_date = ltc.reporting_date
    WHERE population.age BETWEEN 25 AND 84
        OR ltc.person_id IS NOT NULL
),

candidate_keys AS (
    SELECT DISTINCT person_id
    FROM population
),

llt_orders AS (
    SELECT
        orders.person_id,
        orders.medication_order_id,
        orders.order_date,
        orders.lipid_lowering_class,
        orders.bnf_name,
        orders.is_statin,
        orders.statin_intensity
    FROM {{ ref('int_lipid_lowering_medications_all') }} AS orders
    INNER JOIN candidate_keys AS candidate
        ON orders.person_id = candidate.person_id
    WHERE orders.order_date <= (SELECT MAX(reporting_date) FROM population)
        AND orders.lipid_lowering_class IN (
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

first_llt AS (
    SELECT
        person_id,
        MIN(order_date) AS first_order_date
    FROM llt_orders
    GROUP BY person_id
),

llt_daily AS (
    SELECT *
    FROM llt_orders
    -- Preserve statin-first ties before the order-id tie-break.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, order_date::DATE
        ORDER BY order_date DESC, is_statin DESC, medication_order_id DESC
    ) = 1
),

statin_daily AS (
    SELECT
        person_id,
        MAX(order_date) AS order_date
    FROM llt_orders
    WHERE is_statin
    GROUP BY person_id, order_date::DATE
),

ras_orders AS (
    {% for model, ras_class in [('int_ace_inhibitor_medications_all', 'ACE_INHIBITOR'), ('int_arb_medications_all', 'ARB')] %}
    SELECT
        orders.person_id,
        orders.medication_order_id,
        orders.order_date,
        orders.bnf_name,
        '{{ ras_class }}' AS ras_class
    FROM {{ ref(model) }} AS orders
    INNER JOIN candidate_keys AS candidate
        ON orders.person_id = candidate.person_id
    WHERE orders.order_date <= (SELECT MAX(reporting_date) FROM population)
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

ras_daily AS (
    SELECT *
    FROM ras_orders
    WHERE order_date >= '1990-01-01'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, order_date::DATE
        ORDER BY order_date DESC, medication_order_id DESC
    ) = 1
),

ras_sequence_daily AS (
    -- The existing IND324 sequence includes pre-1990 RAS orders, unlike current treatment.
    SELECT
        person_id,
        MAX(order_date) AS order_date
    FROM ras_orders
    GROUP BY person_id, order_date::DATE
),

sglt2_daily AS (
    SELECT
        orders.person_id,
        orders.medication_order_id,
        orders.order_date,
        orders.sglt2_drug,
        orders.bnf_name
    FROM {{ ref('int_sglt2_medications_all') }} AS orders
    INNER JOIN candidate_keys AS candidate
        ON orders.person_id = candidate.person_id
    WHERE orders.order_date BETWEEN '1990-01-01' AND (SELECT MAX(reporting_date) FROM population)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY orders.person_id, orders.order_date::DATE
        ORDER BY orders.order_date DESC, orders.medication_order_id DESC
    ) = 1
),

antiplatelet_daily AS (
    SELECT
        orders.person_id,
        MAX(orders.order_date) AS order_date
    FROM {{ ref('int_antiplatelet_medications_all') }} AS orders
    INNER JOIN candidate_keys AS candidate
        ON orders.person_id = candidate.person_id
    WHERE orders.order_date BETWEEN '1990-01-01' AND (SELECT MAX(reporting_date) FROM population)
    GROUP BY orders.person_id, orders.order_date::DATE
),

anticoagulant_orders AS (
    SELECT
        orders.person_id,
        orders.medication_order_id,
        orders.order_date,
        orders.anticoagulant_type,
        orders.is_doac,
        orders.is_vka
    FROM {{ ref('int_anticoagulant_medications_all') }} AS orders
    INNER JOIN candidate_keys AS candidate
        ON orders.person_id = candidate.person_id
    WHERE orders.order_date BETWEEN '1990-01-01' AND (SELECT MAX(reporting_date) FROM population)
),

anticoagulant_daily AS (
    SELECT *
    FROM anticoagulant_orders
    -- C4 accepts the highest order id when legacy MAX_BY can choose conflicting same-date types.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, order_date::DATE
        ORDER BY order_date DESC, medication_order_id DESC
    ) = 1
),

{% for name, flag in [('doac', 'is_doac'), ('vka', 'is_vka')] %}
{{ name }}_daily AS (
    SELECT
        person_id,
        MAX(order_date) AS order_date
    FROM anticoagulant_orders
    WHERE {{ flag }}
    GROUP BY person_id, order_date::DATE
),

{% endfor %}
{% for name in ['llt', 'statin', 'antiplatelet', 'anticoagulant', 'doac', 'vka', 'ras', 'sglt2'] %}
selected_{{ name }} AS (
    SELECT
        population.person_id,
        population.reporting_date,
        orders.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN {{ name }}_daily AS orders
        MATCH_CONDITION (population.reporting_date >= orders.order_date::DATE)
        ON population.person_id = orders.person_id
),

{% endfor %}
sglt2_anchors AS (
    SELECT DISTINCT
        person_id,
        order_date
    FROM selected_sglt2
    WHERE order_date IS NOT NULL
),

ras_before_sglt2 AS (
    SELECT
        sglt2.person_id,
        sglt2.order_date AS sglt2_order_date,
        ras.order_date AS latest_ras_order_before_last_sglt2_date
    FROM sglt2_anchors AS sglt2
    ASOF JOIN ras_sequence_daily AS ras
        MATCH_CONDITION (sglt2.order_date >= ras.order_date)
        ON sglt2.person_id = ras.person_id
)

SELECT
    population.person_id,
    population.reporting_date,
    llt.order_date AS latest_lipid_lowering_order_date,
    llt.medication_order_id AS latest_lipid_lowering_order_id,
    llt.lipid_lowering_class AS latest_lipid_lowering_class,
    llt.bnf_name AS latest_lipid_lowering_product,
    llt.is_statin AS is_latest_lipid_lowering_statin,
    llt.statin_intensity AS latest_statin_intensity,
    statin.order_date AS latest_statin_order_date,
    first_llt.first_order_date AS first_lipid_lowering_order_date,
    antiplatelet.order_date AS latest_antiplatelet_order_date,
    anticoagulant.order_date AS latest_anticoagulant_order_date,
    anticoagulant.anticoagulant_type AS latest_anticoagulant_type,
    doac.order_date AS latest_doac_order_date,
    vka.order_date AS latest_vka_order_date,
    ras.order_date AS latest_ras_order_date,
    ras.medication_order_id AS latest_ras_order_id,
    ras.ras_class AS latest_ras_class,
    ras.bnf_name AS latest_ras_product,
    sglt2.order_date AS latest_sglt2_order_date,
    sglt2.medication_order_id AS latest_sglt2_order_id,
    sglt2.sglt2_drug AS latest_sglt2_drug,
    sglt2.bnf_name AS latest_sglt2_product,
    sequence.latest_ras_order_before_last_sglt2_date
FROM population
{% for name in ['llt', 'statin', 'antiplatelet', 'anticoagulant', 'doac', 'vka', 'ras', 'sglt2'] %}
LEFT JOIN selected_{{ name }} AS {{ name }}
    ON population.person_id = {{ name }}.person_id
    AND population.reporting_date = {{ name }}.reporting_date
{% endfor %}
LEFT JOIN first_llt
    ON population.person_id = first_llt.person_id
    AND first_llt.first_order_date::DATE <= population.reporting_date
LEFT JOIN ras_before_sglt2 AS sequence
    ON sglt2.person_id = sequence.person_id
    AND sglt2.order_date = sequence.sglt2_order_date
{% endmacro %}
