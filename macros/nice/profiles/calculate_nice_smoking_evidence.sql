{% macro calculate_nice_smoking_evidence(reference='current') %}
{#-
    Select smoking state, never-smoked recording and QOF support for any LTC member or age 43 to 84.
    Args: reference is current or by_month.
    Returns: one candidate person per reporting_date with the named evidence fields.
    Clinical evidence is selected on or before reporting_date; indicators apply their windows.
-#}
-- NICE smoking evidence at each reference date.
WITH population AS (
    {{ nice_reference_population(reference) }}
),

ltc_candidates AS (
    SELECT DISTINCT
        person_id,
        reporting_date
    FROM ({{ nice_ltc_summary(reference) }})
),

candidates AS (
    SELECT
        population.person_id,
        population.reporting_date
    FROM population
    LEFT JOIN ltc_candidates AS ltc
        ON population.person_id = ltc.person_id
        AND population.reporting_date = ltc.reporting_date
    WHERE ltc.person_id IS NOT NULL
        OR population.age BETWEEN 43 AND 84
),

candidate_people AS (
    SELECT DISTINCT person_id
    FROM candidates
),

smoking_daily AS (
    SELECT
        event.person_id,
        event.clinical_effective_date::DATE AS event_date,
        event.smoking_status
    FROM {{ ref('int_smoking_status_all') }} AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM candidates)
    -- QOF status priority precedes observation id within the latest calendar day.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event.person_id, event.clinical_effective_date::DATE
        ORDER BY CASE event.source_cluster_id
            WHEN 'LSMOK_COD' THEN 1
            WHEN 'EXSMOK_COD' THEN 2
            WHEN 'NSMOK_COD' THEN 3
            ELSE 4
        END,
        event.id DESC
    ) = 1
),

never_smoked_daily AS (
    SELECT DISTINCT
        event.person_id,
        event.clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_smoking_status_all') }} AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.is_never_smoked_code
        AND event.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM candidates)
),

support_daily AS (
    SELECT DISTINCT
        event.person_id,
        event.event_date
    FROM {{ ref('int_nice_smoking_support_all') }} AS event
    INNER JOIN candidate_people AS candidate
        ON event.person_id = candidate.person_id
    WHERE event.event_date <= (SELECT MAX(reporting_date) FROM candidates)
),

smoking_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_smoking_status_date,
        event.smoking_status AS latest_smoking_status
    FROM candidates AS candidate
    ASOF JOIN smoking_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
),

never_smoked_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_never_smoked_date
    FROM candidates AS candidate
    ASOF JOIN never_smoked_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
),

support_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_smoking_intervention_date
    FROM candidates AS candidate
    ASOF JOIN support_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
)

SELECT
    candidate.person_id,
    candidate.reporting_date,
    smoking.latest_smoking_status,
    smoking.latest_smoking_status_date,
    never_smoked.latest_never_smoked_date,
    support.latest_smoking_intervention_date
FROM candidates AS candidate
LEFT JOIN smoking_selected AS smoking
    ON candidate.person_id = smoking.person_id
    AND candidate.reporting_date = smoking.reporting_date
LEFT JOIN never_smoked_selected AS never_smoked
    ON candidate.person_id = never_smoked.person_id
    AND candidate.reporting_date = never_smoked.reporting_date
LEFT JOIN support_selected AS support
    ON candidate.person_id = support.person_id
    AND candidate.reporting_date = support.reporting_date
{% endmacro %}
