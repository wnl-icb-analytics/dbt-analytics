{% macro calculate_nice_ckd_profile(reference='current') %}
{#-
    NICE CKD evidence for register members at each reference date.
    Args: reference is current or by_month.
    Returns: the existing profile columns plus reporting_date, at person/date grain.
-#}
-- NICE CKD evidence for register members at each reference date.
WITH population AS (
    SELECT
        person_id,
        reporting_date,
        earliest_diagnosis_date::DATE AS ckd_diagnosis_date
    FROM ({{ nice_register('CKD', reference) }})
),

candidate_keys AS (
    SELECT DISTINCT person_id
    FROM population
),

frailty AS (
    {{ nice_register('FRAIL', reference) }}
),

diabetes AS (
    {{ nice_register('DM', reference) }}
),

hypertension AS (
    {{ nice_register('HTN', reference) }}
),

egfr_daily AS (
    SELECT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS event_date,
        clinical_effective_date::DATE AS latest_egfr_date,
        CASE WHEN is_valid_egfr THEN egfr_value END AS latest_egfr_value
    FROM {{ ref('int_egfr_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE evidence.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM population)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.clinical_effective_date::DATE
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

selected_egfr AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN egfr_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

acr_daily AS (
    SELECT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS event_date,
        clinical_effective_date::DATE AS latest_acr_date,
        acr_value AS latest_acr_value
    FROM {{ ref('int_urine_acr_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE is_acr_ratio AND is_result_recorded
        AND evidence.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM population)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.clinical_effective_date::DATE
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

selected_acr AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN acr_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

proteinuria_first AS (
    SELECT
        evidence.person_id,
        MIN(IFF(record_type IN ('PROTEINURIA', 'CKD_PROTEINURIA', 'PERSISTENT_PROTEINURIA'), clinical_effective_date::DATE, NULL)) AS first_proteinuria_date,
        MIN(IFF(record_type = 'MICROALBUMINURIA', clinical_effective_date::DATE, NULL)) AS first_microalbuminuria_date
    FROM {{ ref('int_proteinuria_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    GROUP BY evidence.person_id
),

contraindication_daily AS (
    SELECT
        evidence.person_id,
        clinical_effective_date::DATE AS event_date,
        MIN(IFF(drug_class = 'ACE_INHIBITOR' AND is_persisting, clinical_effective_date::DATE, NULL)) AS first_ace_persisting_date,
        MIN(IFF(drug_class = 'ARB' AND is_persisting, clinical_effective_date::DATE, NULL)) AS first_arb_persisting_date,
        MAX(IFF(drug_class = 'ACE_INHIBITOR' AND NOT is_persisting, clinical_effective_date::DATE, NULL)) AS latest_ace_expiring_date,
        MAX(IFF(drug_class = 'ARB' AND NOT is_persisting, clinical_effective_date::DATE, NULL)) AS latest_arb_expiring_date
    FROM {{ ref('int_ras_contraindication_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    GROUP BY evidence.person_id, clinical_effective_date::DATE
),

contraindication_state_daily AS (
    SELECT
        person_id,
        event_date,
        MIN(first_ace_persisting_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS first_ace_persisting_date,
        MIN(first_arb_persisting_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS first_arb_persisting_date,
        MAX(latest_ace_expiring_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS latest_ace_expiring_date,
        MAX(latest_arb_expiring_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS latest_arb_expiring_date
    FROM contraindication_daily
),

selected_contraindication_state AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN contraindication_state_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

valued_egfr AS (
    SELECT DISTINCT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS test_date
    FROM {{ ref('int_egfr_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE evidence.egfr_value IS NOT NULL
),

first_egfr AS (
    SELECT
        person_id,
        MIN(test_date) AS first_test_date
    FROM valued_egfr
    GROUP BY person_id
),

egfr_pair AS (
    -- A second test qualifies only if a valued test precedes it by at least 90 days.
    SELECT
        population.person_id,
        population.reporting_date,
        MAX(test.test_date) AS second_egfr_before_diagnosis_date
    FROM population
    INNER JOIN valued_egfr AS test
        ON population.person_id = test.person_id
        AND test.test_date BETWEEN DATEADD(day, -90, population.ckd_diagnosis_date) AND population.ckd_diagnosis_date
        AND test.test_date <= population.reporting_date
    INNER JOIN first_egfr AS first
        ON test.person_id = first.person_id
        AND first.first_test_date <= DATEADD(day, -90, test.test_date)
    GROUP BY population.person_id, population.reporting_date
),

egfr_near_diagnosis AS (
    SELECT
        population.person_id,
        population.reporting_date,
        MAX(test.test_date) AS egfr_within_90_days_of_diagnosis_date
    FROM population
    INNER JOIN valued_egfr AS test
        ON population.person_id = test.person_id
        AND test.test_date BETWEEN DATEADD(day, -90, population.ckd_diagnosis_date)
            AND LEAST(DATEADD(day, 90, population.ckd_diagnosis_date), population.reporting_date)
    GROUP BY population.person_id, population.reporting_date
),

acr_near_diagnosis AS (
    -- Process evidence needs a recorded ratio test, including a value-free test.
    SELECT
        population.person_id,
        population.reporting_date,
        MAX(test.clinical_effective_date::DATE) AS acr_within_90_days_of_diagnosis_date
    FROM population
    INNER JOIN {{ ref('int_urine_acr_all') }} AS test
        ON population.person_id = test.person_id
        AND test.is_acr_ratio
        AND test.clinical_effective_date::DATE BETWEEN DATEADD(day, -90, population.ckd_diagnosis_date)
            AND LEAST(DATEADD(day, 90, population.ckd_diagnosis_date), population.reporting_date)
    GROUP BY population.person_id, population.reporting_date
),

ras_orders AS (
    SELECT
        person_id,
        medication_order_id,
        order_date,
        'ACE_INHIBITOR' AS ras_class
    FROM {{ ref('int_ace_inhibitor_medications_all') }}
    UNION ALL
    SELECT
        person_id,
        medication_order_id,
        order_date,
        'ARB' AS ras_class
    FROM {{ ref('int_arb_medications_all') }}
),

ras_daily AS (
    SELECT
        orders.person_id,
        orders.order_date::DATE AS event_date,
        orders.order_date,
        orders.ras_class
    FROM ras_orders AS orders
    INNER JOIN candidate_keys AS candidate
        ON orders.person_id = candidate.person_id
    WHERE orders.order_date BETWEEN '1990-01-01' AND (SELECT MAX(reporting_date) FROM population)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY orders.person_id, orders.order_date::DATE
        ORDER BY orders.order_date DESC, orders.medication_order_id DESC
    ) = 1
),

selected_ras AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN ras_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

sglt2_daily AS (
    SELECT
        evidence.person_id,
        evidence.order_date::DATE AS event_date,
        order_date,
        sglt2_drug
    FROM {{ ref('int_sglt2_medications_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE evidence.order_date >= '1990-01-01'
        AND evidence.order_date::DATE <= (SELECT MAX(reporting_date) FROM population)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.order_date::DATE
        ORDER BY order_date DESC, medication_order_id DESC
    ) = 1
),

selected_sglt2 AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN sglt2_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

ras_sequence_daily AS (
    -- The baseline sequence includes pre-1990 orders, unlike current treatment.
    SELECT
        orders.person_id,
        MAX(orders.order_date) AS order_date
    FROM ras_orders AS orders
    INNER JOIN candidate_keys AS candidate
        ON orders.person_id = candidate.person_id
    GROUP BY orders.person_id, orders.order_date::DATE
),

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
    population.ckd_diagnosis_date,
    egfr.latest_egfr_date,
    egfr.latest_egfr_value,
    acr.latest_acr_date,
    acr.latest_acr_value,
    COALESCE(proteinuria.first_proteinuria_date <= population.reporting_date, FALSE) AS has_proteinuria,
    COALESCE(proteinuria.first_microalbuminuria_date <= population.reporting_date, FALSE) AS has_microalbuminuria,
    frailty.latest_frailty_severity,
    diabetes.person_id IS NOT NULL AS has_diabetes,
    diabetes.diabetes_type,
    hypertension.person_id IS NOT NULL AS has_hypertension,
    ras.order_date AS latest_ras_order_date,
    sequence.latest_ras_order_before_last_sglt2_date,
    ras.ras_class AS latest_ras_class,
    COALESCE(contraindication.first_ace_persisting_date IS NOT NULL
        OR contraindication.latest_ace_expiring_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_ace_inhibitor_contraindicated,
    COALESCE(contraindication.first_arb_persisting_date IS NOT NULL
        OR contraindication.latest_arb_expiring_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_arb_contraindicated,
    sglt2.order_date AS latest_sglt2_order_date,
    sglt2.sglt2_drug AS latest_sglt2_drug,
    egfr_pair.person_id IS NOT NULL AS has_egfr_pair_before_diagnosis,
    egfr_pair.second_egfr_before_diagnosis_date,
    egfr_near_diagnosis.person_id IS NOT NULL AS has_egfr_within_90_days_of_diagnosis,
    egfr_near_diagnosis.egfr_within_90_days_of_diagnosis_date,
    acr_near_diagnosis.person_id IS NOT NULL AS has_acr_within_90_days_of_diagnosis,
    acr_near_diagnosis.acr_within_90_days_of_diagnosis_date,
    population.reporting_date
FROM population
LEFT JOIN selected_egfr AS egfr
    ON population.person_id = egfr.person_id
    AND population.reporting_date = egfr.reporting_date
LEFT JOIN selected_acr AS acr
    ON population.person_id = acr.person_id
    AND population.reporting_date = acr.reporting_date
LEFT JOIN frailty AS frailty
    ON population.person_id = frailty.person_id
    AND population.reporting_date = frailty.reporting_date
LEFT JOIN diabetes AS diabetes
    ON population.person_id = diabetes.person_id
    AND population.reporting_date = diabetes.reporting_date
LEFT JOIN hypertension AS hypertension
    ON population.person_id = hypertension.person_id
    AND population.reporting_date = hypertension.reporting_date
LEFT JOIN selected_contraindication_state AS contraindication
    ON population.person_id = contraindication.person_id
    AND population.reporting_date = contraindication.reporting_date
LEFT JOIN egfr_pair AS egfr_pair
    ON population.person_id = egfr_pair.person_id
    AND population.reporting_date = egfr_pair.reporting_date
LEFT JOIN egfr_near_diagnosis AS egfr_near_diagnosis
    ON population.person_id = egfr_near_diagnosis.person_id
    AND population.reporting_date = egfr_near_diagnosis.reporting_date
LEFT JOIN acr_near_diagnosis AS acr_near_diagnosis
    ON population.person_id = acr_near_diagnosis.person_id
    AND population.reporting_date = acr_near_diagnosis.reporting_date
LEFT JOIN selected_ras AS ras
    ON population.person_id = ras.person_id
    AND population.reporting_date = ras.reporting_date
LEFT JOIN selected_sglt2 AS sglt2
    ON population.person_id = sglt2.person_id
    AND population.reporting_date = sglt2.reporting_date
LEFT JOIN proteinuria_first AS proteinuria
    ON population.person_id = proteinuria.person_id
LEFT JOIN ras_before_sglt2 AS sequence
    ON sglt2.person_id = sequence.person_id
    AND sglt2.order_date = sequence.sglt2_order_date
{% endmacro %}
