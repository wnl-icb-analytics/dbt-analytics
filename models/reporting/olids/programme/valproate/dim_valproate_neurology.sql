{{ config(
    materialized='table',
    description="Neurology evidence defined for the valproate programme, one row per person with an observation in the NEUROLOGY category of stg_reference_valproate_prog_codes. Contains earliest and latest event dates and the recorded identifiers and codes. No date, current-registration or recent-valproate restriction is applied.") }}

WITH person_level_neurology_aggregation AS (
    SELECT
        person_id,
        min(neurology_event_date) AS earliest_neurology_event_date,
        max(neurology_event_date) AS latest_neurology_event_date,
        array_agg(DISTINCT neurology_ID::VARCHAR)
            AS all_neurology_IDs,
        array_agg(DISTINCT neurology_concept_code)
            AS all_neurology_concept_codes,
        array_agg(DISTINCT neurology_concept_display)
            AS all_neurology_concept_displays,
        array_agg(DISTINCT neurology_code_category)
            AS all_neurology_code_categories_applied
    FROM {{ ref('int_valproate_neurology_events') }}
    GROUP BY person_id
)

SELECT
    pla.person_id,
    TRUE AS has_neurology_event,
    pla.earliest_neurology_event_date,
    pla.latest_neurology_event_date,
    pla.all_neurology_IDs,
    pla.all_neurology_concept_codes,
    pla.all_neurology_concept_displays,
    pla.all_neurology_code_categories_applied
FROM person_level_neurology_aggregation AS pla
-- Brief: Aggregates neurology events and status for valproate cohort, using intermediate neurology events table. Includes latest and historical neurology status, event details, and code traceability.
