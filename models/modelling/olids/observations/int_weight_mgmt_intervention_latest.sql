{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date'], tags=['smi_registry']) }}

select person_id, clinical_effective_date, source_cluster_id, concept_code, concept_display,
    referral_diet_advice, referral_exercise_advice
from {{ ref('int_weight_mgmt_intervention') }}
where intervention_outcome is not null
qualify row_number() over (
    partition by person_id
    order by clinical_effective_date::date desc,
        case intervention_outcome when 'Yes' then 1 when 'Declined' then 2 else 3 end,
        date_recorded desc nulls last, id desc
) = 1
