{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
    )
}}

-- Keep each event type: a same-day invitation must not erase a referral or decline.
SELECT
    obs.person_id,
    obs.clinical_effective_date::DATE AS clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display
FROM ({{ get_observations("'DPPOFF_COD'") }}) AS obs
WHERE obs.clinical_effective_date IS NOT NULL
    AND obs.clinical_effective_date::DATE <= CURRENT_DATE()
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY obs.person_id, obs.mapped_concept_code, obs.clinical_effective_date::DATE
    ORDER BY obs.id
) = 1
