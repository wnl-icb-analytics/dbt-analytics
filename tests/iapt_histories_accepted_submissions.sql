-- A withdrawn submission must not survive the history clean-up. Return counts only.
with retained as (
    select 'stg_iapt_referral_history' as model_name, submission_id
    from {{ ref('stg_iapt_referral_history') }}
    group by submission_id
    union all
    select 'stg_iapt_care_contact_history' as model_name, submission_id
    from {{ ref('stg_iapt_care_contact_history') }}
    group by submission_id
    union all
    select 'stg_iapt_care_activity_history' as model_name, submission_id
    from {{ ref('stg_iapt_care_activity_history') }}
    group by submission_id
    union all
    select 'stg_iapt_onward_referral_history' as model_name, submission_id
    from {{ ref('stg_iapt_onward_referral_history') }}
    group by submission_id
    union all
    select 'stg_iapt_referral_assessment_history' as model_name, submission_id
    from {{ ref('stg_iapt_referral_assessment_history') }}
    group by submission_id
    union all
    select 'stg_iapt_activity_assessment_history' as model_name, submission_id
    from {{ ref('stg_iapt_activity_assessment_history') }}
    group by submission_id
    union all
    select 'stg_iapt_previous_diagnosis_history' as model_name, submission_id
    from {{ ref('stg_iapt_previous_diagnosis_history') }}
    group by submission_id
    union all
    select 'stg_iapt_long_term_condition_history' as model_name, submission_id
    from {{ ref('stg_iapt_long_term_condition_history') }}
    group by submission_id
    union all
    select 'stg_iapt_presenting_complaint_history' as model_name, submission_id
    from {{ ref('stg_iapt_presenting_complaint_history') }}
    group by submission_id
)

select r.model_name, count(*) as withdrawn_submissions
from retained as r
left join {{ ref('stg_iapt_activesubmission') }} as a
    on r.submission_id = a.submission_id
where a.submission_id is null
group by r.model_name
having count(*) > 0
