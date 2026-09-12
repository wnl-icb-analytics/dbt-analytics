-- Every published clinical parent must exist in the fact it names, with the expected type and
-- model, and name the same non-null person and recorded ids as the child. Returns one aggregate
-- row per failing link type; no record identifiers.
with activity_contact as (
    select
        'care_activity_to_care_contact' as parent_link
        , count_if(
            a.parent_record_id is null
            or a.parent_record_type is distinct from 'care_contact'
            or a.parent_model_name is distinct from 'fct_iapt_care_contact'
            or c.source_record_id is null
            or a.person_id is null
            or a.person_id is distinct from c.person_id
            or a.referral_id is distinct from c.referral_id
            or a.care_contact_id is distinct from c.care_contact_id
        ) as invalid_count
    from {{ ref('fct_iapt_care_activity') }} as a
    left join {{ ref('fct_iapt_care_contact') }} as c
        on a.parent_record_id = c.source_record_id
    where coalesce(a.parent_record_id, a.parent_record_type, a.parent_model_name) is not null
)

, assessment_activity as (
    select
        'assessment_to_care_activity' as parent_link
        , count_if(
            s.parent_record_id is null
            or s.parent_record_id is distinct from s.care_activity_source_record_id
            or s.parent_model_name is distinct from 'fct_iapt_care_activity'
            or pa.source_record_id is null
            or s.person_id is null
            or s.person_id is distinct from pa.person_id
            or s.referral_id is distinct from pa.referral_id
            or s.care_contact_id is distinct from pa.care_contact_id
            or s.care_activity_id is distinct from pa.care_activity_id
        ) as invalid_count
    from {{ ref('fct_iapt_assessment_score') }} as s
    left join {{ ref('fct_iapt_care_activity') }} as pa
        on s.parent_record_id = pa.source_record_id
    where s.parent_record_type = 'care_activity'
)

, assessment_referral as (
    select
        'assessment_to_referral' as parent_link
        , count_if(
            s.parent_record_id is null
            or s.parent_record_id is distinct from s.referral_id
            or s.parent_model_name is distinct from 'fct_iapt_referral'
            or r.source_record_id is null
            or s.person_id is null
            or s.person_id is distinct from r.person_id
        ) as invalid_count
    from {{ ref('fct_iapt_assessment_score') }} as s
    left join {{ ref('fct_iapt_referral') }} as r
        on s.parent_record_id = r.source_record_id
    where s.parent_record_type = 'referral'
)

, assessment_shape as (
    select
        'assessment_parent_without_valid_type' as parent_link
        , count(*) as invalid_count
    from {{ ref('fct_iapt_assessment_score') }}
    where coalesce(parent_record_id, parent_record_type, parent_model_name) is not null
        and coalesce(parent_record_type, '') not in ('care_activity', 'referral')
)

, condition_referral as (
    select
        'condition_to_referral' as parent_link
        , count_if(
            h.parent_record_id is null
            or h.parent_record_id is distinct from h.referral_id
            or h.parent_record_type is distinct from 'referral'
            or h.parent_model_name is distinct from 'fct_iapt_referral'
            or r.source_record_id is null
            or h.person_id is null
            or h.person_id is distinct from r.person_id
        ) as invalid_count
    from {{ ref('fct_iapt_health_condition') }} as h
    left join {{ ref('fct_iapt_referral') }} as r
        on h.parent_record_id = r.source_record_id
    where coalesce(h.parent_record_id, h.parent_record_type, h.parent_model_name) is not null
)

select parent_link, invalid_count
from (
    select * from activity_contact
    union all select * from assessment_activity
    union all select * from assessment_referral
    union all select * from assessment_shape
    union all select * from condition_referral
)
where invalid_count > 0
