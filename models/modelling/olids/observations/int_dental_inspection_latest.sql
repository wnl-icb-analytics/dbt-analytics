{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--Latest Dental inspection using mixed codes defined by SMI Longer Lives Campaign covers Dental Inspection.

select *
FROM {{ ref('int_dental_inspection') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1