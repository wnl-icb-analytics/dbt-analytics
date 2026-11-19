{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date'], tags=['smi_registry']) }}

select obs.person_id, obs.clinical_effective_date,
    obs.mapped_concept_code as concept_code,
    obs.mapped_concept_display as concept_display,
    datediff(day, obs.clinical_effective_date, current_date()) <= 365 as med_rev_last_12m,
    obs.id, obs.date_recorded
from ({{ get_observations(observation_evidence_cluster_ids('medication_review'), source='ECL_CACHE') }}) as obs
inner join {{ ref('snomed_evidence_type') }} as classification
    on obs.mapped_concept_code = classification.snomed_code
where obs.clinical_effective_date is not null
    and obs.clinical_effective_date <= current_date()
    and classification.evidence_type = 'completed'
qualify row_number() over (
    partition by obs.person_id, obs.mapped_concept_code, obs.clinical_effective_date
    order by obs.date_recorded desc nulls last, obs.id desc
) = 1
