with timed as (
    select
        a.*
        , count(*) over (
            partition by person_id, provider_organisation_code, referral_source_record_id,
                assessment_context, assessment_concept_code, assessor_id, assessor_local_id, assessment_recorded_at
        ) as n_observations_at_same_time
    from {{ ref('fct_mhsds_assessment_observation') }} as a
    where person_id is not null and assessment_recorded_at is not null
)
, comparable as (
    select *
    from timed
    where n_observations_at_same_time = 1 and assessment_score_numeric is not null
        and not coalesce(has_person_identifier_changed, false)
)
, paired as (
    select
        a.*
        , lag(assessment_observation_id) over (
            partition by person_id, provider_organisation_code, referral_source_record_id,
                assessment_context, assessment_concept_code, assessor_id, assessor_local_id
            order by assessment_recorded_at
        ) as previous_assessment_observation_id
        , lag(assessment_recorded_at) over (
            partition by person_id, provider_organisation_code, referral_source_record_id,
                assessment_context, assessment_concept_code, assessor_id, assessor_local_id
            order by assessment_recorded_at
        ) as previous_assessment_recorded_at
        , lag(assessment_score_numeric) over (
            partition by person_id, provider_organisation_code, referral_source_record_id,
                assessment_context, assessment_concept_code, assessor_id, assessor_local_id
            order by assessment_recorded_at
        ) as previous_score
    from comparable as a

)
select
    assessment_observation_id as assessment_score_change_id
    , assessment_observation_id
    , previous_assessment_observation_id
    , person_id
    , provider_organisation_code
    , provider_organisation_name
    , referral_source_record_id
    , assessment_context
    , assessment_concept_code
    , assessment_concept_description
    , assessment_tool_name
    , assessor_id
    , assessor_local_id
    , coalesce(assessor_id, assessor_local_id) is not null as has_recorded_assessor
    , previous_assessment_recorded_at
    , assessment_recorded_at
    , previous_score
    , assessment_score_numeric as current_score
    , assessment_score_numeric - previous_score as score_change
    , datediff(day, previous_assessment_recorded_at, assessment_recorded_at) as calendar_days_between_scores
    , assessment_definition_version
    , reporting_period_start_date
    , reporting_period_end_date
from paired
where previous_assessment_observation_id is not null
