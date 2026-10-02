{% macro nice_ind120(reference='current') %}
{#-
    Calculate NICE IND120 at each reference date using its reviewed rule.
    Args: reference is current or by_month.
    Returns: the indicator detail columns, one eligible person per reporting_date.
-#}
-- NICE IND120: https://www.nice.org.uk/indicators/ind120
-- NICE corrected the renal process to eGFR creatinine measurement in February 2026.
-- Count HbA1c tests with or without a value, and performed foot and ACR tests
-- anywhere in the period, even if a later foot record is declined or the ACR
-- test has no numeric result.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('DM', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
),

candidate_people AS (
    SELECT DISTINCT person_id
    FROM indicator_population
),

egfr_days AS (
    -- The renal process is an eGFR test, including a test without a value.
    SELECT
        obs.person_id,
        obs.clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_egfr_test_all') }} AS obs
    INNER JOIN candidate_people AS candidate
        ON obs.person_id = candidate.person_id
    GROUP BY obs.person_id, obs.clinical_effective_date::DATE
),

acr_days AS (
    SELECT
        obs.person_id,
        obs.clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_urine_acr_all') }} AS obs
    INNER JOIN candidate_people AS candidate
        ON obs.person_id = candidate.person_id
    WHERE obs.is_acr_ratio
    GROUP BY obs.person_id, obs.clinical_effective_date::DATE
),

cholesterol_days AS (
    -- Retain the care-process report's valid total-cholesterol selection.
    SELECT
        obs.person_id,
        obs.clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_cholesterol_all') }} AS obs
    INNER JOIN candidate_people AS candidate
        ON obs.person_id = candidate.person_id
    WHERE obs.is_valid_cholesterol
    GROUP BY obs.person_id, obs.clinical_effective_date::DATE
),

selected_records AS (
    SELECT
        population.person_id,
        population.reporting_date,
        egfr.event_date AS latest_egfr_date,
        acr.event_date AS latest_acr_date,
        cholesterol.event_date AS latest_cholesterol_date
    FROM indicator_population AS population
    ASOF JOIN egfr_days AS egfr
        MATCH_CONDITION (population.reporting_date >= egfr.event_date)
        ON population.person_id = egfr.person_id
    ASOF JOIN acr_days AS acr
        MATCH_CONDITION (population.reporting_date >= acr.event_date)
        ON population.person_id = acr.person_id
    ASOF JOIN cholesterol_days AS cholesterol
        MATCH_CONDITION (population.reporting_date >= cholesterol.event_date)
        ON population.person_id = cholesterol.person_id
),

performed_foot AS (
    SELECT
        population.person_id,
        population.reporting_date,
        MAX(obs.clinical_effective_date::DATE) AS latest_foot_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_foot_examination_all') }} AS obs
        ON population.person_id = obs.person_id
        AND obs.clinical_effective_date::DATE
            BETWEEN DATEADD(month, -12, population.reporting_date) AND population.reporting_date
        -- A later declined record cannot replace an earlier performed examination.
        -- Missing opposite-foot state applies at R, rather than at the examination date.
        AND (
            obs.both_feet_checked
            OR (obs.left_foot_checked AND (
                obs.first_right_foot_absent_date <= population.reporting_date
                OR obs.first_right_foot_amputated_date <= population.reporting_date))
            OR (obs.right_foot_checked AND (
                obs.first_left_foot_absent_date <= population.reporting_date
                OR obs.first_left_foot_amputated_date <= population.reporting_date))
        )
    GROUP BY population.person_id, population.reporting_date
),

process_dates AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        {{ nice_practice_columns('population', reference) }},
        hba.latest_hba1c_date,
        physical.latest_blood_pressure_date AS latest_bp_date,
        selected.latest_cholesterol_date,
        selected.latest_egfr_date,
        selected.latest_acr_date,
        foot.latest_foot_date,
        -- Age at R permits recorded BMI for children and adult calculated BMI for adults.
        physical.latest_bmi_date,
        smoking.latest_smoking_status_date AS latest_smoking_date
    FROM indicator_population AS population
    LEFT JOIN {{ nice_ref('int_nice_hba1c_evidence', reference) }} AS hba
        ON population.person_id = hba.person_id
        AND population.reporting_date = hba.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_physical_health_evidence', reference) }} AS physical
        ON population.person_id = physical.person_id
        AND population.reporting_date = physical.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_smoking_evidence', reference) }} AS smoking
        ON population.person_id = smoking.person_id
        AND population.reporting_date = smoking.reporting_date
    LEFT JOIN selected_records AS selected
        ON population.person_id = selected.person_id
        AND population.reporting_date = selected.reporting_date
    LEFT JOIN performed_foot AS foot
        ON population.person_id = foot.person_id
        AND population.reporting_date = foot.reporting_date
),

assessed AS (
    SELECT
        person_id,
        reporting_date,
        age,
        {{ nice_practice_columns(none, reference) }},
        IFF(latest_hba1c_date >= DATEADD(month, -12, reporting_date), 1, 0)
            + IFF(latest_bp_date >= DATEADD(month, -12, reporting_date), 1, 0)
            + IFF(latest_cholesterol_date >= DATEADD(month, -12, reporting_date), 1, 0)
            + IFF(latest_egfr_date >= DATEADD(month, -12, reporting_date), 1, 0)
            + IFF(latest_acr_date >= DATEADD(month, -12, reporting_date), 1, 0)
            + IFF(latest_foot_date IS NOT NULL, 1, 0)
            + IFF(latest_bmi_date >= DATEADD(month, -12, reporting_date), 1, 0)
            + IFF(latest_smoking_date >= DATEADD(month, -12, reporting_date), 1, 0)
            AS care_processes_completed_count,
        GREATEST_IGNORE_NULLS(
            latest_bmi_date,
            latest_bp_date,
            CASE WHEN latest_hba1c_date >= DATEADD(month, -12, reporting_date) THEN latest_hba1c_date END,
            latest_cholesterol_date,
            latest_smoking_date,
            latest_foot_date,
            CASE WHEN latest_acr_date >= DATEADD(month, -12, reporting_date) THEN latest_acr_date END,
            CASE WHEN latest_egfr_date >= DATEADD(month, -12, reporting_date) THEN latest_egfr_date END
        )::DATE AS latest_process_date
    FROM process_dates
),

status AS (
    SELECT
        *,
        care_processes_completed_count = 8 AS is_in_numerator,
        CASE WHEN care_processes_completed_count = 8 THEN latest_process_date END AS latest_record_date
    FROM assessed
)

SELECT
    person_id,
    'IND120' AS indicator_id,
    'Diabetes: annual general practice checks' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Diabetes' AS condition_name,
    {{ nice_practice_columns(none, reference) }},
    care_processes_completed_count::NUMBER(17, 0) AS care_processes_completed_count,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM status
{% endmacro %}
