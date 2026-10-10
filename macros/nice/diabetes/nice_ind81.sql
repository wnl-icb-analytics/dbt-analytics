{% macro nice_ind81(reference='current') %}
{#-
    Calculate NICE IND81 at each reference date using its reviewed rule.
    Args: reference is current or by_month.
    Returns: the indicator detail columns, one eligible person per reporting_date.
-#}
-- NICE IND81: https://www.nice.org.uk/indicators/ind81
-- Foot examination with a risk classification in 15 months on the diabetes register, allowing an absent or amputated foot in the examination but excluding anyone with a foot amputation.
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

foot_state AS (
    SELECT
        person_id,
        MIN(first_left_foot_amputated_date) AS first_left_foot_amputated_date,
        MIN(first_right_foot_amputated_date) AS first_right_foot_amputated_date
    FROM {{ ref('int_foot_examination_all') }}
    GROUP BY person_id
),

eligible_population AS (
    SELECT population.*
    FROM indicator_population AS population
    LEFT JOIN foot_state AS foot
        ON population.person_id = foot.person_id
    -- NICE excludes any amputation known through its clinical date by R.
    WHERE NOT COALESCE(foot.first_left_foot_amputated_date <= population.reporting_date, FALSE)
        AND NOT COALESCE(foot.first_right_foot_amputated_date <= population.reporting_date, FALSE)
),

qualifying_examination AS (
    SELECT
        population.person_id,
        population.reporting_date,
        MAX(obs.clinical_effective_date::DATE) AS latest_record_date,
        MAX_BY(obs.diabetes_foot_risk_category, obs.clinical_effective_date) AS latest_foot_risk_category
    FROM eligible_population AS population
    INNER JOIN {{ ref('int_foot_examination_all') }} AS obs
        ON population.person_id = obs.person_id
        AND obs.clinical_effective_date::DATE
            BETWEEN DATEADD(month, -15, population.reporting_date) AND population.reporting_date
        AND obs.has_risk_classification
        -- The opposite foot may be absent by R, even when absence follows the examination.
        AND (
            obs.both_feet_checked
            OR (obs.left_foot_checked AND obs.first_right_foot_absent_date <= population.reporting_date)
            OR (obs.right_foot_checked AND obs.first_left_foot_absent_date <= population.reporting_date)
        )
    GROUP BY population.person_id, population.reporting_date
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        {{ nice_practice_columns('population', reference) }},
        exam.latest_record_date,
        exam.latest_foot_risk_category,
        exam.person_id IS NOT NULL AS is_in_numerator
    FROM eligible_population AS population
    LEFT JOIN qualifying_examination AS exam
        ON population.person_id = exam.person_id
        AND population.reporting_date = exam.reporting_date
)

SELECT
    person_id,
    'IND81' AS indicator_id,
    'Diabetes: annual foot exam and risk classification' AS indicator_name,
    'The percentage of patients with diabetes with a record of a foot examination and risk classification: 1) low risk (normal sensation, palpable pulses), 2) increased risk (neuropathy or absent pulses), 3) high risk (neuropathy or absent pulses plus deformity or skin changes or previous ulcer) or 4) ulcerated foot within the preceding 15 months.' AS indicator_description,
    reporting_date,
    DATEADD(month, -15, reporting_date) AS measurement_period_start,
    age,
    'Diabetes without a foot amputation' AS denominator_description,
    {{ nice_practice_columns(none, reference) }},
    latest_foot_risk_category,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
