{% macro calculate_nice_cvd_risk_profile(reference='current') %}
{#-
    NICE CVD risk evidence for active people aged 25-84 or on DM/SMI registers.
    Args: reference is current or by_month.
    Returns: the existing profile columns plus reporting_date, at person/date grain.
-#}
-- NICE CVD risk evidence for active people aged 25-84 or on DM/SMI registers.
WITH reference_bounds AS (
    SELECT
        MIN(reporting_date) AS earliest_reporting_date,
        MAX(reporting_date) AS latest_reporting_date
    FROM ({{ nice_reference_dates(reference) }}) AS dates
),

diabetes AS (
    {{ nice_register('DM', reference) }}
),

smi AS (
    {{ nice_register('SMI', reference) }}
),

fh AS (
    {{ nice_register('FH', reference) }}
),

ckd AS (
    {{ nice_register('CKD', reference) }}
),

hypertension AS (
    {{ nice_register('HTN', reference) }}
),

frailty AS (
    {{ nice_register('FRAIL', reference) }}
),

obesity AS (
    {{ nice_register('OB', reference) }}
),

population AS (
    SELECT
        population.person_id,
        population.reporting_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    LEFT JOIN diabetes
        ON population.person_id = diabetes.person_id
        AND population.reporting_date = diabetes.reporting_date
    LEFT JOIN smi
        ON population.person_id = smi.person_id
        AND population.reporting_date = smi.reporting_date
    -- C1 authorises the same NICE candidate restriction for the current profile.
    WHERE population.age BETWEEN 25 AND 84
        OR diabetes.person_id IS NOT NULL
        OR smi.person_id IS NOT NULL
),

candidate_keys AS (
    SELECT DISTINCT person_id
    FROM population
),

qrisk_daily AS (
    SELECT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS event_date,
        qrisk_score AS latest_risk_score,
        clinical_effective_date::DATE AS latest_risk_score_date,
        qrisk_type AS latest_risk_score_type
    FROM {{ ref('int_qrisk_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE is_valid_qrisk
        AND evidence.clinical_effective_date::DATE <= (SELECT latest_reporting_date FROM reference_bounds)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.clinical_effective_date::DATE
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

qrisk_daily_extremes AS (
    SELECT
        evidence.person_id,
        clinical_effective_date::DATE AS event_date,
        MAX(qrisk_score) AS day_max_score,
        MIN(qrisk_score) AS day_min_score
    FROM {{ ref('int_qrisk_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE is_valid_qrisk
        AND evidence.clinical_effective_date::DATE <= (SELECT latest_reporting_date FROM reference_bounds)
    GROUP BY evidence.person_id, clinical_effective_date::DATE
),

qrisk_state_daily AS (
    -- Daily extrema retain all scores; latest selection retains its timestamp/id tie order.
    SELECT
        latest.person_id,
        latest.event_date,
        latest.latest_risk_score,
        latest.latest_risk_score_date,
        latest.latest_risk_score_type,
        MAX(extremes.day_max_score) OVER (
            PARTITION BY latest.person_id ORDER BY latest.event_date ROWS UNBOUNDED PRECEDING
        ) AS max_risk_score_ever
    FROM qrisk_daily AS latest
    INNER JOIN qrisk_daily_extremes AS extremes
        ON latest.person_id = extremes.person_id
        AND latest.event_date = extremes.event_date
),

selected_qrisk AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.latest_risk_score,
        evidence.latest_risk_score_date,
        evidence.latest_risk_score_type,
        evidence.max_risk_score_ever
    FROM population
    ASOF JOIN qrisk_state_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

score_window AS (
    -- Window maxima/minima differ from latest selection and must retain all qualifying days.
    SELECT
        population.person_id,
        population.reporting_date,
        MAX(IFF(score.event_date >= DATEADD(month, -12, population.reporting_date), score.day_max_score, NULL)) AS max_risk_score_12m,
        MIN(score.day_min_score) AS min_risk_score_36m
    -- Missing windows stay absent here; the final left join supplies their null values.
    FROM population
    INNER JOIN qrisk_daily_extremes AS score
        ON population.person_id = score.person_id
        AND score.event_date BETWEEN DATEADD(month, -36, population.reporting_date) AND population.reporting_date
    WHERE score.event_date >= DATEADD(month, -36, (SELECT earliest_reporting_date FROM reference_bounds))
    GROUP BY population.person_id, population.reporting_date
),

cvd_score_daily AS (
    SELECT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS event_date,
        clinical_effective_date::DATE AS latest_cvd_risk_score_date,
        CASE WHEN risk_score_value BETWEEN 0 AND 100 THEN risk_score_value END AS latest_cvd_risk_score
    FROM {{ ref('int_cvd_risk_assessment_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE is_cvd_risk_score_code AND original_result_value IS NOT NULL
        AND evidence.clinical_effective_date::DATE <= (SELECT latest_reporting_date FROM reference_bounds)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.clinical_effective_date::DATE
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

selected_cvd_score AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN cvd_score_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

assessment_daily AS (
    SELECT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS event_date,
        clinical_effective_date::DATE AS latest_risk_assessment_date
    FROM {{ ref('int_cvd_risk_assessment_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE evidence.clinical_effective_date::DATE <= (SELECT latest_reporting_date FROM reference_bounds)
    -- Only the date is returned, so same-day records need no identifier sort.
    GROUP BY evidence.person_id, evidence.clinical_effective_date::DATE
),

selected_assessment AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN assessment_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

assessment_daily_extremes AS (
    SELECT
        evidence.person_id,
        clinical_effective_date::DATE AS event_date,
        MAX(IFF(is_cvd_risk_score_code AND risk_score_value BETWEEN 0 AND 100, risk_score_value, NULL)) AS day_max_score,
        MAX(IFF(is_cvd_risk_score_code AND risk_score_value >= 0 AND risk_score_value < 10, clinical_effective_date::DATE, NULL)) AS low_score_date,
        MAX(IFF(is_cvd_risk_score_code AND risk_score_value BETWEEN 10 AND 100, clinical_effective_date::DATE, NULL)) AS high_score_date
    FROM {{ ref('int_cvd_risk_assessment_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    -- Non-score records cannot contribute to these window outputs.
    WHERE is_cvd_risk_score_code AND risk_score_value BETWEEN 0 AND 100
        AND evidence.clinical_effective_date::DATE BETWEEN
            DATEADD(month, -36, (SELECT earliest_reporting_date FROM reference_bounds))
            AND (SELECT latest_reporting_date FROM reference_bounds)
    GROUP BY evidence.person_id, clinical_effective_date::DATE
),

assessment_window AS (
    SELECT
        population.person_id,
        population.reporting_date,
        MAX(IFF(score.event_date >= DATEADD(month, -12, population.reporting_date), score.day_max_score, NULL)) AS max_cvd_risk_score_12m,
        MAX(score.low_score_date) AS latest_low_cvd_risk_score_date_36m,
        MAX(score.high_score_date) AS latest_high_cvd_risk_score_date_36m
    FROM population
    INNER JOIN assessment_daily_extremes AS score
        ON population.person_id = score.person_id
        AND score.event_date BETWEEN DATEADD(month, -36, population.reporting_date) AND population.reporting_date
    GROUP BY population.person_id, population.reporting_date
),

cholesterol_daily AS (
    SELECT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS event_date,
        cholesterol_value AS latest_total_cholesterol,
        clinical_effective_date::DATE AS latest_total_cholesterol_date
    FROM {{ ref('int_cholesterol_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE is_valid_cholesterol
        AND evidence.clinical_effective_date::DATE <= (SELECT latest_reporting_date FROM reference_bounds)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.clinical_effective_date::DATE
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

selected_cholesterol AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN cholesterol_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

smoking_daily AS (
    SELECT
        evidence.person_id,
        evidence.clinical_effective_date::DATE AS event_date,
        smoking_status
    FROM {{ ref('int_smoking_status_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    WHERE evidence.clinical_effective_date::DATE <= (SELECT latest_reporting_date FROM reference_bounds)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.clinical_effective_date::DATE
        ORDER BY CASE source_cluster_id WHEN 'LSMOK_COD' THEN 1 WHEN 'EXSMOK_COD' THEN 2 WHEN 'NSMOK_COD' THEN 3 ELSE 4 END, id DESC
    ) = 1
),

selected_smoking AS (
    SELECT
        population.person_id,
        population.reporting_date,
        evidence.* EXCLUDE (person_id)
    FROM population
    ASOF JOIN smoking_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),

cvd AS (
    SELECT
        person_id,
        reporting_date,
        has_chd,
        has_pad,
        has_stroke_tia,
        has_haemorrhagic_stroke
    FROM {{ nice_ref('int_cvd_secondary_prevention_population', reference) }} AS profile
),

therapy AS (
    SELECT
        person_id,
        reporting_date,
        latest_lipid_lowering_order_date,
        latest_statin_order_date
    FROM {{ nice_ref('int_nice_therapy_evidence', reference) }} AS profile
)

SELECT
    population.person_id,
    score.latest_risk_score,
    score.latest_risk_score_date,
    score.latest_risk_score_type,
    score.max_risk_score_ever,
    score_window.max_risk_score_12m,
    score_window.min_risk_score_36m,
    assessment.latest_risk_assessment_date,
    cvd_score.latest_cvd_risk_score,
    cvd_score.latest_cvd_risk_score_date,
    assessment_window.max_cvd_risk_score_12m,
    assessment_window.latest_low_cvd_risk_score_date_36m,
    assessment_window.latest_high_cvd_risk_score_date_36m,
    COALESCE(cvd.has_chd OR cvd.has_pad
        OR (cvd.has_stroke_tia AND NOT cvd.has_haemorrhagic_stroke), FALSE) AS has_cvd,
    COALESCE(cvd.has_chd OR cvd.has_pad OR cvd.has_stroke_tia, FALSE) AS has_cvd_including_haemorrhagic_stroke,
    fh.person_id IS NOT NULL AS has_familial_hypercholesterolaemia,
    ckd.person_id IS NOT NULL AS has_ckd,
    diabetes.person_id IS NOT NULL AS has_diabetes,
    COALESCE(diabetes.diabetes_type = 'Type 1', FALSE) AS has_type1_diabetes,
    COALESCE(diabetes.diabetes_type = 'Type 2', FALSE) AS has_type2_diabetes,
    diabetes.earliest_type2_date::DATE AS earliest_type2_diabetes_date,
    hypertension.person_id IS NOT NULL AS has_hypertension,
    hypertension.earliest_diagnosis_date::DATE AS earliest_hypertension_date,
    frailty.latest_frailty_severity,
    therapy.latest_lipid_lowering_order_date,
    therapy.latest_statin_order_date,
    COALESCE(smoking.smoking_status = 'Current Smoker', FALSE) AS is_current_smoker,
    obesity.person_id IS NOT NULL AS has_obesity,
    cholesterol.latest_total_cholesterol,
    cholesterol.latest_total_cholesterol_date,
    population.reporting_date
FROM population
LEFT JOIN selected_qrisk AS score
    ON population.person_id = score.person_id
    AND population.reporting_date = score.reporting_date
LEFT JOIN score_window AS score_window
    ON population.person_id = score_window.person_id
    AND population.reporting_date = score_window.reporting_date
LEFT JOIN selected_assessment AS assessment
    ON population.person_id = assessment.person_id
    AND population.reporting_date = assessment.reporting_date
LEFT JOIN selected_cvd_score AS cvd_score
    ON population.person_id = cvd_score.person_id
    AND population.reporting_date = cvd_score.reporting_date
LEFT JOIN assessment_window AS assessment_window
    ON population.person_id = assessment_window.person_id
    AND population.reporting_date = assessment_window.reporting_date
LEFT JOIN cvd AS cvd
    ON population.person_id = cvd.person_id
    AND population.reporting_date = cvd.reporting_date
LEFT JOIN fh AS fh
    ON population.person_id = fh.person_id
    AND population.reporting_date = fh.reporting_date
LEFT JOIN ckd AS ckd
    ON population.person_id = ckd.person_id
    AND population.reporting_date = ckd.reporting_date
LEFT JOIN diabetes AS diabetes
    ON population.person_id = diabetes.person_id
    AND population.reporting_date = diabetes.reporting_date
LEFT JOIN hypertension AS hypertension
    ON population.person_id = hypertension.person_id
    AND population.reporting_date = hypertension.reporting_date
LEFT JOIN frailty AS frailty
    ON population.person_id = frailty.person_id
    AND population.reporting_date = frailty.reporting_date
LEFT JOIN therapy AS therapy
    ON population.person_id = therapy.person_id
    AND population.reporting_date = therapy.reporting_date
LEFT JOIN selected_smoking AS smoking
    ON population.person_id = smoking.person_id
    AND population.reporting_date = smoking.reporting_date
LEFT JOIN obesity AS obesity
    ON population.person_id = obesity.person_id
    AND population.reporting_date = obesity.reporting_date
LEFT JOIN selected_cholesterol AS cholesterol
    ON population.person_id = cholesterol.person_id
    AND population.reporting_date = cholesterol.reporting_date
{% endmacro %}
