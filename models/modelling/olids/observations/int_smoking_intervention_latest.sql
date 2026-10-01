{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--Latest Smoking intervention using mixed codes defined by SMI Longer Lives Campaign covers Smoking Cessation Interventions.

select *
FROM {{ ref('int_smoking_intervention') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1