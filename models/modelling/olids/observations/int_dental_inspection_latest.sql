{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date'], tags=['smi_registry']) }}

select person_id, clinical_effective_date, concept_code, concept_display, dental_last_12m
from {{ ref('int_dental_inspection') }}
qualify row_number() over (
    partition by person_id
    order by clinical_effective_date::date desc, date_recorded desc nulls last, id desc
) = 1
