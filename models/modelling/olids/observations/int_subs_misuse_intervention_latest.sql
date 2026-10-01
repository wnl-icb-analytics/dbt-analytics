{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
----using mixed codes defined by SMI Longer Lives Campaign covers Substance Misuse Interventions.
select *
FROM {{ ref('int_subs_misuse_intervention') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1