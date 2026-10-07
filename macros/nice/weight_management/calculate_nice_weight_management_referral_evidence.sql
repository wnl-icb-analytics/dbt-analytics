{% macro calculate_nice_weight_management_referral_evidence(reference='current') %}
{#-
    Select timely weight management evidence for NICE obesity register members.
    Args: reference is current or by_month.
    Returns: one person per reporting_date with BMI, referral and attendance dates.
-#}
WITH population AS (
    SELECT register.person_id,
        {% if reference == 'current' %}register.reporting_date{% else %}register.month_end_date{% endif %} AS reporting_date,
        register.latest_bmi_date, register.bmi_value, register.bmi_source,
        register.requires_lower_bmi_thresholds, register.bmi_category
    FROM {{ ref('fct_person_nice_obesity_register' if reference == 'current' else 'fct_person_nice_obesity_register_by_month') }} AS register
), people AS (
    SELECT DISTINCT person_id FROM population
), heights AS (
    -- Calculated BMI cannot be known before its contributing height was recorded.
    SELECT obs.person_id, obs.clinical_effective_date, obs.date_recorded
    FROM ({{ get_observations("'HEIGHT'") }}) AS obs
    INNER JOIN people ON obs.person_id = people.person_id
    WHERE obs.clinical_effective_date IS NOT NULL
        AND TRY_CAST(obs.result_value AS FLOAT) BETWEEN 50 AND 250
        AND obs.age_at_event >= 18
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY obs.person_id, obs.clinical_effective_date ORDER BY obs.id DESC
    ) = 1
), bmi_records AS (
    SELECT bmi.person_id, bmi.clinical_effective_date::DATE AS bmi_date, bmi.bmi_value,
        IFF(bmi.bmi_source = 'calculated',
            GREATEST({{ ltc_known_date('bmi.clinical_effective_date', 'bmi.date_recorded') }},
                COALESCE(height.date_recorded::DATE, bmi.clinical_effective_date::DATE)),
            {{ ltc_known_date('bmi.clinical_effective_date', 'bmi.date_recorded') }}) AS known_date
    FROM {{ ref('int_bmi_all') }} AS bmi
    INNER JOIN people ON bmi.person_id = people.person_id
    ASOF JOIN heights AS height
        MATCH_CONDITION (bmi.clinical_effective_date >= height.clinical_effective_date)
        ON bmi.person_id = height.person_id
    WHERE bmi.bmi_value BETWEEN 10 AND 150
        AND bmi.clinical_effective_date::DATE > DATEADD(month, -12, (SELECT MIN(reporting_date) FROM population))
), anchors AS (
    SELECT population.person_id, population.reporting_date, bmi.bmi_date
    FROM population
    INNER JOIN bmi_records AS bmi ON population.person_id = bmi.person_id
        AND bmi.bmi_date > DATEADD(month, -12, population.reporting_date)
        AND bmi.known_date <= population.reporting_date
    WHERE {{ bmi_category('bmi.bmi_value', 'population.requires_lower_bmi_thresholds') }} LIKE 'Obese%'
    GROUP BY population.person_id, population.reporting_date, bmi.bmi_date
), first_qualifying_bmi AS (
    SELECT person_id, reporting_date,
        {% if reference == 'current' %}
        -- Current measures retain the evidence snapshot but assess cohort expiry today.
        MIN(IFF(bmi_date > DATEADD(month, -12, CURRENT_DATE()::DATE), bmi_date, NULL))
        {% else %}
        MIN(bmi_date)
        {% endif %} AS earliest_qualifying_bmi_date
    FROM anchors
    GROUP BY person_id, reporting_date
), timely AS (
    SELECT anchors.person_id, anchors.reporting_date,
        MAX(IFF(events.is_referral, events.event_date, NULL)) AS timely_referral_date,
        MAX(IFF(events.is_offer, events.event_date, NULL)) AS timely_offer_date,
        MAX(IFF(events.is_decline, events.event_date, NULL)) AS timely_decline_date
    FROM anchors
    INNER JOIN {{ ref('int_nice_weight_management_events_all') }} AS events
        ON anchors.person_id = events.person_id
        AND events.event_date >= anchors.bmi_date
        AND events.event_date <= DATEADD(day, 90, anchors.bmi_date)
        AND events.event_date <= anchors.reporting_date
    WHERE events.is_referral OR events.is_offer OR events.is_decline
    GROUP BY anchors.person_id, anchors.reporting_date
), status_dates AS (
    SELECT population.person_id, population.reporting_date,
        MAX(IFF(events.is_referral, events.event_date, NULL)) AS latest_referral_date,
        MAX(IFF(events.is_attendance AND events.event_date > DATEADD(month, -12, population.reporting_date),
            events.event_date, NULL)) AS latest_attendance_date,
        MAX(IFF(events.is_end, events.event_date, NULL)) AS latest_end_date
    FROM population
    INNER JOIN {{ ref('int_nice_weight_management_events_all') }} AS events
        ON population.person_id = events.person_id
        AND events.event_date > DATEADD(month, -24, population.reporting_date)
        AND events.event_date <= population.reporting_date
    GROUP BY population.person_id, population.reporting_date
)
SELECT population.person_id, population.reporting_date,
    population.latest_bmi_date, population.bmi_value, population.bmi_source,
    population.requires_lower_bmi_thresholds, population.bmi_category,
    -- The register supplies a qualifying BMI even when its event is absent from the retained input.
    COALESCE(first_bmi.earliest_qualifying_bmi_date,
        {% if reference == 'current' %}
        IFF(population.latest_bmi_date > DATEADD(month, -12, CURRENT_DATE()::DATE)
            AND population.latest_bmi_date <= CURRENT_DATE()::DATE, population.latest_bmi_date, NULL)
        {% else %}
        population.latest_bmi_date
        {% endif %}) AS earliest_qualifying_bmi_date,
    timely.timely_referral_date, timely.timely_offer_date, timely.timely_decline_date,
    status_dates.latest_referral_date, status_dates.latest_attendance_date, status_dates.latest_end_date,
    status_dates.latest_referral_date IS NOT NULL AS has_previous_referral,
    status_dates.latest_attendance_date IS NOT NULL
        AND (status_dates.latest_end_date IS NULL OR status_dates.latest_end_date <= status_dates.latest_attendance_date)
        AS is_currently_attending
FROM population
LEFT JOIN first_qualifying_bmi AS first_bmi ON population.person_id = first_bmi.person_id
    AND population.reporting_date = first_bmi.reporting_date
LEFT JOIN timely ON population.person_id = timely.person_id
    AND population.reporting_date = timely.reporting_date
LEFT JOIN status_dates ON population.person_id = status_dates.person_id
    AND population.reporting_date = status_dates.reporting_date
{% endmacro %}
