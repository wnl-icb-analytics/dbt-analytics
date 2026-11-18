{% macro calculate_nice_physical_health_evidence(reference='current') %}
{#-
    Select physical-health recording and lithium levels for DM, SMI or recent-lithium candidates.
    Args: reference is current or by_month.
    Returns: one candidate person per reporting_date with the named evidence fields.
    Clinical evidence is selected on or before reporting_date; indicators apply their windows.
-#}
-- NICE physical health evidence at each reference date.
WITH population AS (
    {{ nice_reference_population(reference) }}
),

register_candidates AS (
    {% for condition in ['DM', 'SMI'] %}
    SELECT
        person_id,
        reporting_date
    FROM ({{ nice_register(condition, reference) }})
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

lithium_candidates AS (
    -- A recent order is a safe candidate superset. IND87 applies the independent stop rule.
    SELECT
        population.person_id,
        population.reporting_date
    FROM population
    INNER JOIN {{ ref('int_lithium_medications_all') }} AS lithium
        ON population.person_id = lithium.person_id
        AND lithium.order_date::DATE > DATEADD(month, -6, population.reporting_date)
        AND lithium.order_date::DATE <= population.reporting_date
        AND (lithium.date_recorded IS NULL OR lithium.date_recorded::DATE <= population.reporting_date)
    GROUP BY population.person_id, population.reporting_date
),

candidate_keys AS (
    SELECT
        person_id,
        reporting_date
    FROM register_candidates
    UNION
    SELECT
        person_id,
        reporting_date
    FROM lithium_candidates
),

candidates AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age
    FROM population
    INNER JOIN candidate_keys AS candidate
        ON population.person_id = candidate.person_id
        AND population.reporting_date = candidate.reporting_date
),

candidate_people AS (
    SELECT DISTINCT person_id
    FROM candidates
),

bmi_events AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_bmi_qof_all') }}
    WHERE source_cluster_id = 'BMIVAL_COD'
        AND bmi_value BETWEEN 10 AND 150
),

bmi_daily AS (
    SELECT DISTINCT
        event.person_id,
        event.event_date
    FROM bmi_events AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.event_date <= (SELECT MAX(reporting_date) FROM candidates)
),

calculated_bmi_daily AS (
    -- Adult source-age height/weight classification is retained; no current BMI categories are used.
    SELECT DISTINCT
        event.person_id,
        event.clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_bmi_all') }} AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.is_valid_bmi
        AND event.bmi_source = 'calculated'
        AND event.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM candidates)
),

bmi_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_bmi_date
    FROM candidates AS candidate
    ASOF JOIN bmi_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
),

calculated_bmi_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_calculated_bmi_date
    FROM candidates AS candidate
    ASOF JOIN calculated_bmi_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
    WHERE candidate.age >= 18
),

blood_pressure_events AS (
    -- Recording requires a valid pair, unlike NICE BP-control evidence which retains invalid pairs.
    SELECT
        person_id,
        effective_date::DATE AS event_date
    FROM {{ ref('int_blood_pressure_all') }}
),

blood_pressure_daily AS (
    SELECT DISTINCT
        event.person_id,
        event.event_date
    FROM blood_pressure_events AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.event_date <= (SELECT MAX(reporting_date) FROM candidates)
),

blood_pressure_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_blood_pressure_date
    FROM candidates AS candidate
    ASOF JOIN blood_pressure_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
),

total_cholesterol_events AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_cholesterol_all') }}
    WHERE cholesterol_value IS NOT NULL
),

total_cholesterol_daily AS (
    SELECT DISTINCT
        event.person_id,
        event.event_date
    FROM total_cholesterol_events AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.event_date <= (SELECT MAX(reporting_date) FROM candidates)
),

total_cholesterol_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_total_cholesterol_date
    FROM candidates AS candidate
    ASOF JOIN total_cholesterol_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
),

cholesterol_hdl_ratio_events AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_cholesterol_hdl_ratio_all') }}
    WHERE cholesterol_hdl_ratio IS NOT NULL
),

cholesterol_hdl_ratio_daily AS (
    SELECT DISTINCT
        event.person_id,
        event.event_date
    FROM cholesterol_hdl_ratio_events AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.event_date <= (SELECT MAX(reporting_date) FROM candidates)
),

cholesterol_hdl_ratio_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_cholesterol_hdl_ratio_date
    FROM candidates AS candidate
    ASOF JOIN cholesterol_hdl_ratio_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
),

additional_lipid_events AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_cholesterol_hdl_all') }}
    WHERE cholesterol_value IS NOT NULL

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_cholesterol_ldl_all') }}
    WHERE cholesterol_value IS NOT NULL

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_cholesterol_non_hdl_all') }}
    WHERE cholesterol_value IS NOT NULL

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_triglycerides_all') }}
    WHERE triglycerides_value IS NOT NULL

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_non_numeric_lipid_test_all') }}
),

additional_lipid_daily AS (
    SELECT DISTINCT
        event.person_id,
        event.event_date
    FROM additional_lipid_events AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.event_date <= (SELECT MAX(reporting_date) FROM candidates)
),

additional_lipid_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_additional_lipid_date
    FROM candidates AS candidate
    ASOF JOIN additional_lipid_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
),

glucose_or_hba1c_events AS (
    -- A performed test counts even when it has no numeric result.
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_hba1c_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_blood_glucose_all') }}
),

glucose_or_hba1c_daily AS (
    SELECT DISTINCT
        event.person_id,
        event.event_date
    FROM glucose_or_hba1c_events AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.event_date <= (SELECT MAX(reporting_date) FROM candidates)
),

glucose_or_hba1c_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_glucose_or_hba1c_date
    FROM candidates AS candidate
    ASOF JOIN glucose_or_hba1c_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
),

alcohol_record_events AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_alcohol_units_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_alcohol_usage_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_alcohol_screening_all') }}
),

alcohol_record_daily AS (
    SELECT DISTINCT
        event.person_id,
        event.event_date
    FROM alcohol_record_events AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.event_date <= (SELECT MAX(reporting_date) FROM candidates)
),

alcohol_record_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_alcohol_record_date
    FROM candidates AS candidate
    ASOF JOIN alcohol_record_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
),

lithium_daily AS (
    SELECT
        event.person_id,
        event.clinical_effective_date::DATE AS event_date,
        event.lithium_level,
        event.is_in_therapeutic_range
    FROM {{ ref('int_lithium_level_all') }} AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM candidates)
    -- Retain the hub's timestamp, recorded-result and observation-id priority within a day.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event.person_id, event.clinical_effective_date::DATE
        ORDER BY event.clinical_effective_date DESC, event.is_result_recorded DESC, event.id DESC
    ) = 1
),

lithium_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_lithium_level_date,
        event.lithium_level AS latest_lithium_level,
        event.is_in_therapeutic_range AS is_latest_lithium_level_in_range
    FROM candidates AS candidate
    ASOF JOIN lithium_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
)

SELECT
    candidate.person_id,
    candidate.reporting_date,
    GREATEST_IGNORE_NULLS(bmi.latest_bmi_date, calculated_bmi.latest_calculated_bmi_date) AS latest_bmi_date,
    blood_pressure.latest_blood_pressure_date,
    total_cholesterol.latest_total_cholesterol_date,
    cholesterol_hdl_ratio.latest_cholesterol_hdl_ratio_date,
    GREATEST_IGNORE_NULLS(total_cholesterol.latest_total_cholesterol_date,
        cholesterol_hdl_ratio.latest_cholesterol_hdl_ratio_date,
        additional_lipid.latest_additional_lipid_date) AS latest_lipid_date,
    glucose_or_hba1c.latest_glucose_or_hba1c_date,
    alcohol_record.latest_alcohol_record_date,
    lithium.latest_lithium_level_date,
    lithium.latest_lithium_level,
    COALESCE(lithium.is_latest_lithium_level_in_range, FALSE) AS is_latest_lithium_level_in_range
FROM candidates AS candidate
LEFT JOIN bmi_selected AS bmi
    ON candidate.person_id = bmi.person_id
    AND candidate.reporting_date = bmi.reporting_date
LEFT JOIN blood_pressure_selected AS blood_pressure
    ON candidate.person_id = blood_pressure.person_id
    AND candidate.reporting_date = blood_pressure.reporting_date
LEFT JOIN total_cholesterol_selected AS total_cholesterol
    ON candidate.person_id = total_cholesterol.person_id
    AND candidate.reporting_date = total_cholesterol.reporting_date
LEFT JOIN cholesterol_hdl_ratio_selected AS cholesterol_hdl_ratio
    ON candidate.person_id = cholesterol_hdl_ratio.person_id
    AND candidate.reporting_date = cholesterol_hdl_ratio.reporting_date
LEFT JOIN additional_lipid_selected AS additional_lipid
    ON candidate.person_id = additional_lipid.person_id
    AND candidate.reporting_date = additional_lipid.reporting_date
LEFT JOIN glucose_or_hba1c_selected AS glucose_or_hba1c
    ON candidate.person_id = glucose_or_hba1c.person_id
    AND candidate.reporting_date = glucose_or_hba1c.reporting_date
LEFT JOIN alcohol_record_selected AS alcohol_record
    ON candidate.person_id = alcohol_record.person_id
    AND candidate.reporting_date = alcohol_record.reporting_date
LEFT JOIN calculated_bmi_selected AS calculated_bmi
    ON candidate.person_id = calculated_bmi.person_id
    AND candidate.reporting_date = calculated_bmi.reporting_date
LEFT JOIN lithium_selected AS lithium
    ON candidate.person_id = lithium.person_id
    AND candidate.reporting_date = lithium.reporting_date
{% endmacro %}
