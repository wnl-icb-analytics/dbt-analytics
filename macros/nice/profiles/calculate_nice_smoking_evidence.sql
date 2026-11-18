{% macro calculate_nice_smoking_evidence(reference='current') %}
{#-
    Select smoking state, never-smoked recording and QOF support for any LTC member or age 15+.
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
        OR population.age >= 15
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

unsuitable_daily AS (
    SELECT DISTINCT
        event.person_id,
        event.event_date
    FROM {{ ref('int_nice_smoking_unsuitability_all') }} AS event
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

ex_smoker_candidates AS (
    -- Recent status already counts. Only stale ex-smokers need the lifetime allowance.
    SELECT person_id, reporting_date
    FROM smoking_selected
    WHERE latest_smoking_status = 'Ex-Smoker'
        AND latest_smoking_status_date < DATEADD(month, -12, reporting_date)
),

smoker_days AS (
    SELECT event.person_id, event.clinical_effective_date::DATE AS event_date,
        {{ ltc_known_date('event.clinical_effective_date', 'event.date_recorded') }} AS known_date
    FROM {{ ref('int_smoking_status_all') }} AS event
    INNER JOIN candidate_people AS candidate ON event.person_id = candidate.person_id
    WHERE event.is_smoker_code
    GROUP BY event.person_id, event.clinical_effective_date::DATE,
        {{ ltc_known_date('event.clinical_effective_date', 'event.date_recorded') }}
),

last_smoker AS (
    SELECT candidate.person_id, candidate.reporting_date,
        MAX(event.event_date) AS latest_smoker_date
    FROM ex_smoker_candidates AS candidate
    LEFT JOIN smoker_days AS event ON candidate.person_id = event.person_id
        AND event.known_date <= candidate.reporting_date
    GROUP BY candidate.person_id, candidate.reporting_date
),

ex_smoker_years AS (
    -- NICE specifies financial years; QOF requires no smoker code since the first ex-smoker record.
    SELECT candidate.person_id, candidate.reporting_date,
        YEAR(event.clinical_effective_date) - IFF(MONTH(event.clinical_effective_date) < 4, 1, 0)
            AS financial_year
    FROM last_smoker AS candidate
    INNER JOIN {{ ref('int_smoking_status_all') }} AS event
        ON candidate.person_id = event.person_id AND event.is_ex_smoker_code
        AND {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'candidate.reporting_date') }}
        AND (candidate.latest_smoker_date IS NULL
            OR event.clinical_effective_date::DATE > candidate.latest_smoker_date)
    GROUP BY candidate.person_id, candidate.reporting_date, financial_year
),

ex_smoker_runs AS (
    SELECT person_id, reporting_date, financial_year,
        LAG(financial_year, 2) OVER (
            PARTITION BY person_id, reporting_date ORDER BY financial_year
        ) AS first_financial_year
    FROM ex_smoker_years
),

ex_smoker_covered AS (
    SELECT person_id, reporting_date,
        BOOLOR_AGG(financial_year - first_financial_year = 2) AS is_ex_smoker_covered
    FROM ex_smoker_runs
    GROUP BY person_id, reporting_date
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
),

unsuitable_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        event.event_date AS latest_smoking_unsuitable_date
    FROM candidates AS candidate
    ASOF JOIN unsuitable_daily AS event
        MATCH_CONDITION (candidate.reporting_date >= event.event_date)
        ON candidate.person_id = event.person_id
)

SELECT
    candidate.person_id,
    candidate.reporting_date,
    smoking.latest_smoking_status,
    smoking.latest_smoking_status_date,
    never_smoked.latest_never_smoked_date,
    COALESCE(ex_smoker.is_ex_smoker_covered, FALSE) AS is_ex_smoker_covered,
    support.latest_smoking_intervention_date,
    unsuitable.latest_smoking_unsuitable_date
FROM candidates AS candidate
LEFT JOIN smoking_selected AS smoking
    ON candidate.person_id = smoking.person_id
    AND candidate.reporting_date = smoking.reporting_date
LEFT JOIN never_smoked_selected AS never_smoked
    ON candidate.person_id = never_smoked.person_id
    AND candidate.reporting_date = never_smoked.reporting_date
LEFT JOIN ex_smoker_covered AS ex_smoker
    ON candidate.person_id = ex_smoker.person_id
    AND candidate.reporting_date = ex_smoker.reporting_date
LEFT JOIN support_selected AS support
    ON candidate.person_id = support.person_id
    AND candidate.reporting_date = support.reporting_date
LEFT JOIN unsuitable_selected AS unsuitable
    ON candidate.person_id = unsuitable.person_id
    AND candidate.reporting_date = unsuitable.reporting_date
{% endmacro %}
