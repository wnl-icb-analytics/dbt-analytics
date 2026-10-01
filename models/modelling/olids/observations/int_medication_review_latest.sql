{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--Latest Med Review using mixed codes defined by SMI Longer Lives Campaign covers Medication Review.

select *
FROM {{ ref('int_medication_review') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1