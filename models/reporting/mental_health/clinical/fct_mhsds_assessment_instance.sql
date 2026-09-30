with keyed as (
    select
        a.*
        , {{ dbt_utils.generate_surrogate_key(['provider_organisation_code','person_id','assessment_context',
            'source_assessment_id','referral_source_record_id','care_activity_source_record_id',
            'assessment_recorded_at','assessor_id','assessor_local_id',
            'coalesce(assessment_tool_name, assessment_concept_code)',
            "iff(assessment_recorded_at is null and source_assessment_id is null, assessment_observation_id, null)"]) }} as assessment_instance_id
    from {{ ref('fct_mhsds_assessment_observation') }} as a
)
select
    assessment_instance_id
    , person_id
    , provider_organisation_code
    , max(provider_organisation_name) as provider_organisation_name
    , assessment_context
    , source_assessment_id
    , referral_source_record_id
    , care_activity_source_record_id
    , assessment_tool_name
    , coalesce(assessment_tool_name, assessment_concept_code) as assessment_tool_group
    , assessment_recorded_at
    , assessor_id
    , assessor_local_id
    , case
        when source_assessment_id is not null and assessment_tool_name is null
            then 'source_clustering_assessment_and_concept'
        when source_assessment_id is not null then 'source_clustering_assessment_and_tool'
        when assessment_recorded_at is null then 'undated_source_observation'
        when assessment_tool_name is null then 'shared_concept_context_time_and_assessor'
        else 'shared_tool_context_time_and_assessor'
    end as instance_identity_basis
    , count(*) as n_observations
    , count(distinct assessment_concept_code) as n_distinct_concepts
    , count(assessment_score_numeric) as n_numeric_scores
    , count_if(assessment_response_status = 'known_non_score') as n_known_non_score_responses
    , count_if(assessment_response_status in ('response_unmatched','reference_not_available')) as n_unmatched_responses
    , count(*) > count(distinct assessment_concept_code) as has_repeated_or_missing_concepts
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as reporting_period_end_date
from keyed
group by assessment_instance_id, person_id, provider_organisation_code, assessment_context,
    source_assessment_id, referral_source_record_id, care_activity_source_record_id,
    assessment_tool_name, coalesce(assessment_tool_name, assessment_concept_code),
    assessment_recorded_at, assessor_id, assessor_local_id
