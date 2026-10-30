{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using mixed codes defined by SMI Longer Lives Campaign covers exercise habits.
SELECT * EXCLUDE (observation_id, date_recorded)
FROM {{ ref('int_exercise_assessment') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date::DATE DESC, date_recorded DESC NULLS LAST, observation_id DESC) = 1