{% macro calculate_nice_alcohol_evidence(reference='current') %}
{#-
    Select alcohol screening and screen-relative intervention for relevant LTC and child-depression candidates.
    Args: reference is current or by_month.
    Returns: one candidate person per reporting_date with the named evidence fields.
    Clinical evidence is selected on or before reporting_date; indicators apply their windows.
-#}
-- NICE alcohol evidence at each reference date.
WITH population AS (
    {{ nice_reference_population(reference) }}
),

register_candidates AS (
    {% for condition in ['HTN', 'DEP', 'ANX', 'SMI', 'CHD', 'AF', 'HF', 'STIA', 'DM', 'DEM'] %}
    SELECT
        person_id,
        reporting_date
    FROM ({{ nice_register(condition, reference) }})
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

child_depression_first_known AS (
    -- Candidate superset only. Indicators apply the unresolved first/new child-depression rule.
    SELECT
        person_id,
        MIN({{ ltc_known_date('clinical_effective_date', 'date_recorded') }}) AS first_known_date
    FROM {{ ref('int_depression_diagnoses_all') }}
    WHERE is_diagnosis_code
        AND is_first_or_new_episode
    GROUP BY person_id
),

candidate_keys AS (
    SELECT
        person_id,
        reporting_date
    FROM register_candidates
    UNION
    SELECT
        population.person_id,
        population.reporting_date
    FROM population
    INNER JOIN child_depression_first_known AS diagnosis
        ON population.person_id = diagnosis.person_id
        AND diagnosis.first_known_date <= population.reporting_date
    WHERE population.age BETWEEN 10 AND 17
),

candidates AS (
    SELECT
        population.person_id,
        population.reporting_date
    FROM population
    INNER JOIN candidate_keys AS candidate
        ON population.person_id = candidate.person_id
        AND population.reporting_date = candidate.reporting_date
),

candidate_people AS (
    SELECT DISTINCT person_id
    FROM candidates
),

screen_daily AS (
    SELECT
        screen.person_id,
        screen.clinical_effective_date::DATE AS screen_date,
        screen.screening_tool,
        screen.score_value,
        screen.is_positive_screen
    FROM {{ ref('int_alcohol_screening_all') }} AS screen
    INNER JOIN candidate_people AS candidate
        ON screen.person_id = candidate.person_id
    WHERE screen.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM candidates)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY screen.person_id, screen.clinical_effective_date::DATE
        ORDER BY screen.clinical_effective_date DESC, screen.id DESC, screen.source_cluster_id
    ) = 1
),

positive_screen_daily AS (
    -- Select positive FAST/AUDIT-C separately; a later negative screen does not erase this evidence.
    SELECT DISTINCT
        screen.person_id,
        screen.clinical_effective_date::DATE AS screen_date
    FROM {{ ref('int_alcohol_screening_all') }} AS screen
    INNER JOIN candidate_people AS candidate
        ON screen.person_id = candidate.person_id
    WHERE screen.is_positive_screen
        AND screen.screening_tool IN ('FAST', 'AUDIT-C')
        AND screen.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM candidates)
),

latest_screen AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        screen.screen_date AS latest_alcohol_screen_date,
        screen.screening_tool AS latest_alcohol_screen_tool,
        screen.score_value AS latest_alcohol_screen_score,
        screen.is_positive_screen AS is_latest_alcohol_screen_positive
    FROM candidates AS candidate
    ASOF JOIN screen_daily AS screen
        MATCH_CONDITION (candidate.reporting_date >= screen.screen_date)
        ON candidate.person_id = screen.person_id
),

latest_positive AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        screen.screen_date AS latest_positive_alcohol_screen_date
    FROM candidates AS candidate
    ASOF JOIN positive_screen_daily AS screen
        MATCH_CONDITION (candidate.reporting_date >= screen.screen_date)
        ON candidate.person_id = screen.person_id
),

intervention_daily AS (
    SELECT DISTINCT
        intervention.person_id,
        intervention.clinical_effective_date::DATE AS intervention_date
    FROM {{ ref('int_alcohol_intervention') }} AS intervention
    INNER JOIN candidate_people AS candidate
        ON intervention.person_id = candidate.person_id
    WHERE intervention.alcohol_advice_services = 'Yes'
        AND intervention.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM candidates)
),

intervention_after_positive AS (
    SELECT
        positive.person_id,
        positive.reporting_date,
        MAX(intervention.intervention_date) AS latest_intervention_after_positive_screen_date
    FROM latest_positive AS positive
    LEFT JOIN intervention_daily AS intervention
        ON positive.person_id = intervention.person_id
        -- Only the selected positive screen anchors this field; future interventions cannot hide earlier ones.
        AND intervention.intervention_date BETWEEN positive.latest_positive_alcohol_screen_date
            AND LEAST(DATEADD(month, 3, positive.latest_positive_alcohol_screen_date), positive.reporting_date)
    GROUP BY positive.person_id, positive.reporting_date
)

SELECT
    candidate.person_id,
    candidate.reporting_date,
    screen.latest_alcohol_screen_date,
    screen.latest_alcohol_screen_tool,
    screen.latest_alcohol_screen_score,
    COALESCE(screen.is_latest_alcohol_screen_positive, FALSE) AS is_latest_alcohol_screen_positive,
    positive.latest_positive_alcohol_screen_date,
    intervention.latest_intervention_after_positive_screen_date
FROM candidates AS candidate
LEFT JOIN latest_screen AS screen
    ON candidate.person_id = screen.person_id
    AND candidate.reporting_date = screen.reporting_date
LEFT JOIN latest_positive AS positive
    ON candidate.person_id = positive.person_id
    AND candidate.reporting_date = positive.reporting_date
LEFT JOIN intervention_after_positive AS intervention
    ON candidate.person_id = intervention.person_id
    AND candidate.reporting_date = intervention.reporting_date
{% endmacro %}
