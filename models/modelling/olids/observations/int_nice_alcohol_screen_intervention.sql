{{ config(materialized='table', cluster_by=['person_id', 'screen_date']) }}

-- Interpret NICE IND197/199/200/202 as any qualifying pair; NICE does not select the first or latest screen.
WITH positive_screens AS (
    SELECT
        id AS screening_observation_id,
        source_cluster_id,
        person_id,
        clinical_effective_date::DATE AS screen_date,
        screening_tool,
        score_value
    FROM {{ ref('int_alcohol_screening_all') }}
    WHERE ((screening_tool = 'FAST' AND score_value >= 3)
        OR (screening_tool = 'AUDIT-C' AND score_value >= 5))
        AND clinical_effective_date::DATE <= CURRENT_DATE()
),

interventions AS (
    SELECT DISTINCT person_id, clinical_effective_date::DATE AS intervention_date
    FROM {{ ref('int_alcohol_intervention') }}
    WHERE alcohol_advice_services = 'Yes'
        AND clinical_effective_date::DATE <= CURRENT_DATE()
)

SELECT
    screen.screening_observation_id,
    screen.source_cluster_id,
    screen.person_id,
    screen.screen_date,
    screen.screening_tool,
    screen.score_value,
    MAX(intervention.intervention_date) AS latest_intervention_date
FROM positive_screens AS screen
LEFT JOIN interventions AS intervention
    ON screen.person_id = intervention.person_id
    AND intervention.intervention_date
        BETWEEN screen.screen_date AND DATEADD(month, 3, screen.screen_date)
GROUP BY screen.screening_observation_id, screen.source_cluster_id,
    screen.person_id, screen.screen_date, screen.screening_tool, screen.score_value
