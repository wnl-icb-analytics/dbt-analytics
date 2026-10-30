{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date'], tags=['smi_registry']) }}

with exercise_membership as (
    select distinct code
    from {{ ref('stg_reference_combined_codesets') }}
    where upper(cluster_id) in ({{ observation_evidence_cluster_ids('weight_management', 'PCD') }})
), evidence as (
    select obs.id, obs.person_id, obs.clinical_effective_date, obs.date_recorded,
        obs.cluster_id as source_cluster_id,
        obs.mapped_concept_code as concept_code,
        obs.mapped_concept_display as concept_display,
        classification.evidence_type,
        lower(classification.concept_name) as wording,
        exercise_membership.code is not null as is_national_exercise,
        case
            when classification.evidence_type = 'declined' then 'Declined'
            when not classification.is_offer
                and not classification.is_planned_or_awaited
                and classification.evidence_type in ('completed', 'requested_or_referred', 'advised') then 'Yes'
        end as intervention_outcome
    from ({{ get_observations(observation_evidence_cluster_ids('weight_management')) }}) as obs
    inner join {{ ref('snomed_evidence_type') }} as classification
        on obs.mapped_concept_code = classification.snomed_code
    left join exercise_membership on obs.mapped_concept_code = exercise_membership.code
    where obs.clinical_effective_date is not null
        and obs.clinical_effective_date <= current_date()
)
select person_id, clinical_effective_date, source_cluster_id, concept_code, concept_display,
    iff(regexp_like(wording, '.*(diet|nutriti).*'), intervention_outcome, null)::varchar(16777216) as referral_diet_advice,
    iff(is_national_exercise or regexp_like(wording, '.*(exercise|physical activit|activity tolerance|activity group).*'), intervention_outcome, null)::varchar(16777216) as referral_exercise_advice,
    evidence_type, intervention_outcome, id, date_recorded
from evidence
qualify row_number() over (
    partition by person_id, concept_code, clinical_effective_date
    order by date_recorded desc nulls last, id desc, source_cluster_id
) = 1
