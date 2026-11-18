{{ config(
    materialized='table',
    description="Psychiatry evidence for the OLIDS valproate programme, one row per person with an observation coded in the programme's PSYCH category. No date, current-registration or recent-valproate filter applies.") }}

WITH person_level_psych_aggregation AS (
    SELECT
        person_id,
        min(psych_event_date) AS earliest_psych_event_date,
        max(psych_event_date) AS latest_psych_event_date,
        array_agg(DISTINCT psych_ID::VARCHAR) AS all_psych_IDs,
        array_agg(DISTINCT psych_concept_code) AS all_psych_concept_codes,
        array_agg(DISTINCT psych_concept_display) AS all_psych_concept_displays,
        array_agg(DISTINCT psych_code_category)
            AS all_psych_code_categories_applied
    FROM {{ ref('int_valproate_psychiatry_events') }}
    GROUP BY person_id
)

SELECT
    pla.person_id,
    TRUE AS has_psych_event,
    pla.earliest_psych_event_date,
    pla.latest_psych_event_date,
    pla.all_psych_IDs,
    pla.all_psych_concept_codes,
    pla.all_psych_concept_displays,
    pla.all_psych_code_categories_applied
FROM person_level_psych_aggregation AS pla
-- Brief: Aggregates psychiatry events and status for valproate cohort, using intermediate psychiatry events table. Includes latest and historical psychiatry status, event details, and code traceability.
