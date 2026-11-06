{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry'],
        persist_docs={"relation": true})
}}

/*
Latest cervical screening prompt per person.
Simple QUALIFY-based latest record selection from int_cervical_screening_prompt_all.

Business Rules:
- Returns the most recent screening prompt observation per person
- No date-based business logic (kept in fact layer)
*/

SELECT
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display
FROM {{ ref('int_cervical_screening_prompt_all') }}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id ORDER BY clinical_effective_date::DATE DESC, date_recorded DESC NULLS LAST, observation_id DESC
) = 1

ORDER BY person_id