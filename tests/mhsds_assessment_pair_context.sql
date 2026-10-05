select count(*) as inconsistent_pairs
from {{ ref('fct_mhsds_assessment_score_change') }} as p
left join {{ ref('fct_mhsds_assessment_observation') }} as previous
    on p.previous_assessment_observation_id = previous.assessment_observation_id
where previous.assessment_observation_id is null
    or p.person_id is distinct from previous.person_id
    or p.provider_organisation_code is distinct from previous.provider_organisation_code
    or p.referral_source_record_id is distinct from previous.referral_source_record_id
    or p.assessment_context is distinct from previous.assessment_context
    or p.assessment_concept_code is distinct from previous.assessment_concept_code
    or p.assessor_id is distinct from previous.assessor_id
    or p.assessor_local_id is distinct from previous.assessor_local_id
    or p.previous_assessment_recorded_at >= p.assessment_recorded_at
having inconsistent_pairs > 0
