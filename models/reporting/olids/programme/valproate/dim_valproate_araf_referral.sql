{{ config(
    materialized='table',
    description="ARAF referral evidence for the valproate programme, one row per person with a referral coded in the REFERRAL category of stg_reference_valproate_prog_codes. Includes earliest and latest referral dates, identifiers and codes from referral requests and observations. Duplicate copies of the same referral are removed upstream. No current-registration or recent-valproate restriction is applied.") }}

WITH person_level_araf_referral_aggregation AS (
    SELECT
        person_id,
        min(araf_referral_event_date) AS earliest_araf_referral_event_date,
        max(araf_referral_event_date) AS latest_araf_referral_event_date,
        array_agg(DISTINCT araf_referral_ID::VARCHAR)
            AS all_araf_referral_IDs,
        array_agg(DISTINCT araf_referral_concept_code)
            AS all_araf_referral_concept_codes,
        array_agg(DISTINCT araf_referral_concept_display)
            AS all_araf_referral_concept_displays,
        array_agg(DISTINCT araf_referral_code_category)
            AS all_araf_referral_code_categories_applied
    FROM {{ ref('int_valproate_araf_referral_events') }}
    GROUP BY person_id
)

SELECT
    pla.person_id,
    TRUE AS has_araf_referral_event,
    pla.earliest_araf_referral_event_date,
    pla.latest_araf_referral_event_date,
    pla.all_araf_referral_IDs,
    pla.all_araf_referral_concept_codes,
    pla.all_araf_referral_concept_displays,
    pla.all_araf_referral_code_categories_applied
FROM person_level_araf_referral_aggregation AS pla
-- Brief: Aggregates ARAF referral events and status for valproate cohort, using intermediate ARAF referral events table. Includes latest and historical ARAF referral status, event details, and code traceability.
