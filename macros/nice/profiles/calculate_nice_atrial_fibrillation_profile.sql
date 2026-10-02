{% macro calculate_nice_atrial_fibrillation_profile(reference='current') %}
{#-
    NICE AF evidence for register members at each reference date.
    Args: reference is current or by_month.
    Returns: the existing profile columns plus reporting_date, at person/date grain.
-#}
-- NICE AF evidence for register members at each reference date.
WITH population AS (
    SELECT
        person_id,
        reporting_date,
        earliest_diagnosis_date::DATE AS earliest_af_diagnosis_date
    FROM ({{ nice_register('AF', reference) }})
),

candidate_keys AS (
    SELECT DISTINCT person_id
    FROM population
),

chadsvasc_daily AS (
    SELECT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS event_date,
        score_value AS latest_chadsvasc_score,
        clinical_effective_date::DATE AS latest_chadsvasc_date
    FROM {{ ref('int_stroke_risk_score_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE score_type = 'CHA2DS2-VASc'
        AND evidence.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM population)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.clinical_effective_date::DATE
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

selected_chadsvasc AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN chadsvasc_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

chads2_daily AS (
    SELECT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS event_date,
        score_value AS latest_chads2_score,
        clinical_effective_date::DATE AS latest_chads2_date
    FROM {{ ref('int_stroke_risk_score_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE score_type = 'CHADS2'
        -- QOF v51 retains legacy CHADS2 assessments only before April 2015.
        AND clinical_effective_date::DATE < '2015-04-01'
        AND evidence.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM population)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.clinical_effective_date::DATE
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

selected_chads2 AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN chads2_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

score_daily AS (
    SELECT
        evidence.person_id,
        clinical_effective_date::DATE AS event_date,
        MAX(score_value) AS day_max_score
    FROM {{ ref('int_stroke_risk_score_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    GROUP BY evidence.person_id, clinical_effective_date::DATE
),

score_state_daily AS (
    SELECT
        person_id,
        event_date,
        MAX(day_max_score) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS max_stroke_risk_score_ever
    FROM score_daily
),

selected_score_state AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN score_state_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

selected_score_state_before_period AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN score_state_daily AS evidence
        MATCH_CONDITION (DATEADD(month, -12, population.reporting_date) > evidence.event_date)
        ON population.person_id = evidence.person_id
),

ttr_daily AS (
    SELECT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS event_date,
        ttr_percentage AS latest_ttr_percentage
    FROM {{ ref('int_ttr_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE original_result_value IS NOT NULL
        AND evidence.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM population)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.clinical_effective_date::DATE
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

selected_ttr AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN ttr_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
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
    SELECT
        person_id,
        order_date::DATE AS event_date,
        order_date,
        anticoagulant_type
    FROM anticoagulant_orders
    -- Use order id to resolve equal-date types; current MAX_BY ties are checked in parity.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, order_date::DATE
        ORDER BY order_date DESC, medication_order_id DESC
    ) = 1
),

selected_anticoagulant AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN anticoagulant_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

doac_daily AS (
    SELECT
        person_id,
        order_date::DATE AS event_date,
        MAX(order_date) AS order_date
    FROM anticoagulant_orders
    WHERE is_doac
    GROUP BY person_id, order_date::DATE
),

selected_doac AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN doac_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

vka_daily AS (
    SELECT
        person_id,
        order_date::DATE AS event_date,
        MAX(order_date) AS order_date
    FROM anticoagulant_orders
    WHERE is_vka
    GROUP BY person_id, order_date::DATE
),

selected_vka AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN vka_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

exception_daily AS (
    SELECT
        evidence.person_id,
        clinical_effective_date::DATE AS event_date,
        MIN(IFF(exception_type = 'ANTICOAGULANT_ADVERSE_REACTION', clinical_effective_date::DATE, NULL)) AS first_adverse_reaction_date,
        MIN(IFF(exception_type = 'ANTICOAGULANT_PERSISTING_CONTRAINDICATION', clinical_effective_date::DATE, NULL)) AS first_persisting_contraindication_date,
        MAX(IFF(exception_type = 'ANTICOAGULANT_CONTRAINDICATED', clinical_effective_date::DATE, NULL)) AS latest_anticoagulant_contraindicated_date,
        MAX(IFF(exception_type = 'ANTICOAGULANT_DECLINED', clinical_effective_date::DATE, NULL)) AS latest_anticoagulant_declined_date,
        MIN(IFF(exception_type = 'VALVULAR_AF', clinical_effective_date::DATE, NULL)) AS first_valvular_af_date,
        MIN(IFF(exception_type IN ('DOAC_CONTRAINDICATED', 'ANTIPHOSPHOLIPID_SYNDROME'), clinical_effective_date::DATE, NULL)) AS first_doac_exception_date,
        MAX(IFF(exception_type = 'DOAC_DECLINED', clinical_effective_date::DATE, NULL)) AS latest_doac_declined_date,
        MAX(IFF(exception_type = 'DOAC_NOT_INDICATED', clinical_effective_date::DATE, NULL)) AS latest_doac_not_indicated_date
    FROM {{ ref('int_anticoagulant_exception_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    GROUP BY evidence.person_id, clinical_effective_date::DATE
),

exception_state_daily AS (
    SELECT
        person_id,
        event_date,
        MIN(first_adverse_reaction_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS first_adverse_reaction_date,
        MIN(first_persisting_contraindication_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS first_persisting_contraindication_date,
        MAX(latest_anticoagulant_contraindicated_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS latest_anticoagulant_contraindicated_date,
        MAX(latest_anticoagulant_declined_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS latest_anticoagulant_declined_date,
        MIN(first_valvular_af_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS first_valvular_af_date,
        MIN(first_doac_exception_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS first_doac_exception_date,
        MAX(latest_doac_declined_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS latest_doac_declined_date,
        MAX(latest_doac_not_indicated_date) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS latest_doac_not_indicated_date
    FROM exception_daily
),

selected_exception_state AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN exception_state_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

review_daily AS (
    SELECT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS event_date,
        clinical_effective_date::DATE AS latest_anticoagulant_review_date
    FROM {{ ref('int_anticoagulant_review_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE evidence.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM population)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.clinical_effective_date::DATE
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

selected_review AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN review_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
)

SELECT
    population.person_id,
    population.earliest_af_diagnosis_date,
    chadsvasc.latest_chadsvasc_score,
    chadsvasc.latest_chadsvasc_date,
    chads2.latest_chads2_score,
    chads2.latest_chads2_date,
    GREATEST_IGNORE_NULLS(chadsvasc.latest_chadsvasc_date, chads2.latest_chads2_date) AS latest_stroke_risk_score_date,
    CASE
        WHEN chadsvasc.latest_chadsvasc_date >= chads2.latest_chads2_date
            OR chads2.latest_chads2_date IS NULL THEN chadsvasc.latest_chadsvasc_score
        ELSE chads2.latest_chads2_score
    END AS latest_stroke_risk_score,
    history.max_stroke_risk_score_ever,
    before_period.max_stroke_risk_score_ever AS max_stroke_risk_score_before_period,
    therapy.order_date AS latest_anticoagulant_order_date,
    therapy.anticoagulant_type AS latest_anticoagulant_type,
    doac.order_date AS latest_doac_order_date,
    vka.order_date AS latest_vka_order_date,
    exceptions.first_adverse_reaction_date IS NOT NULL AS has_anticoagulant_adverse_reaction,
    exceptions.first_persisting_contraindication_date IS NOT NULL AS has_anticoagulant_persisting_contraindication,
    exceptions.latest_anticoagulant_contraindicated_date,
    exceptions.latest_anticoagulant_declined_date,
    exceptions.first_valvular_af_date IS NOT NULL AS is_doac_ineligible,
    -- Preserve IND247's persisting exception and separate 12-month decline/not-indicated rules.
    COALESCE(exceptions.first_doac_exception_date IS NOT NULL
        OR exceptions.latest_doac_declined_date >= DATEADD(month, -12, population.reporting_date), FALSE)
        OR COALESCE(exceptions.latest_doac_not_indicated_date >= DATEADD(month, -12, population.reporting_date)
            AND ttr.event_date >= DATEADD(month, -6, population.reporting_date)
            AND ttr.latest_ttr_percentage >= 65, FALSE) AS has_doac_exception,
    review.latest_anticoagulant_review_date,
    population.reporting_date
FROM population
LEFT JOIN selected_chadsvasc AS chadsvasc
    ON population.person_id = chadsvasc.person_id
    AND population.reporting_date = chadsvasc.reporting_date
LEFT JOIN selected_chads2 AS chads2
    ON population.person_id = chads2.person_id
    AND population.reporting_date = chads2.reporting_date
LEFT JOIN selected_score_state AS history
    ON population.person_id = history.person_id
    AND population.reporting_date = history.reporting_date
LEFT JOIN selected_score_state_before_period AS before_period
    ON population.person_id = before_period.person_id
    AND population.reporting_date = before_period.reporting_date
LEFT JOIN selected_ttr AS ttr
    ON population.person_id = ttr.person_id
    AND population.reporting_date = ttr.reporting_date
LEFT JOIN selected_anticoagulant AS therapy
    ON population.person_id = therapy.person_id
    AND population.reporting_date = therapy.reporting_date
LEFT JOIN selected_doac AS doac
    ON population.person_id = doac.person_id
    AND population.reporting_date = doac.reporting_date
LEFT JOIN selected_vka AS vka
    ON population.person_id = vka.person_id
    AND population.reporting_date = vka.reporting_date
LEFT JOIN selected_exception_state AS exceptions
    ON population.person_id = exceptions.person_id
    AND population.reporting_date = exceptions.reporting_date
LEFT JOIN selected_review AS review
    ON population.person_id = review.person_id
    AND population.reporting_date = review.reporting_date
{% endmacro %}
