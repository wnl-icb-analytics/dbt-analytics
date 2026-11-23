{{
    config(
        materialized='table',
        cluster_by=['person_id'],
         tags=['screening_programme'],
        persist_docs={"relation": true})
}}

/*
Latest bowel screening status per person (completed or declined).
Simple QUALIFY-based latest record selection from int_bowel_screening_all.

Business Rules:
- Returns the most recent screening observation per person
- No date-based business logic (kept in fact layer)
- Foundation for person-level screening status analysis

Used for bowel screening programme analysis and current status determination.
*/

SELECT
    ID,
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    screening_observation_type,
    is_completed_screening,
    -- is_unsuitable_screening,
    is_declined_screening,
    -- is_non_response_screening

FROM {{ ref('int_bowel_screening_all') }}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id 
    ORDER BY clinical_effective_date::DATE DESC, date_recorded DESC NULLS LAST, ID DESC, source_cluster_id DESC
) = 1

ORDER BY person_id