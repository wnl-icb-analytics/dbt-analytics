with contact_context as (
    select
        c.mhs201_uniq_id
        , c.uniq_submission_id
        , r.mhs101_uniq_id as same_submission_referral_record_id
        , r.person_id as same_submission_referral_person_id
        , r.referral_request_received_date as same_submission_referral_received_date
        , r.mhs101_uniq_id is not null as is_same_submission_referral_linked
        , c.person_id = r.person_id as is_same_submission_referral_person_consistent
        , {{ mhsds_delivering_team_basis(
            'c.other_care_prof_team_local_id', 'c.care_prof_team_local_id',
            'r.care_prof_team_local_id', 's.dat_set_ver'
        ) }} as service_or_team_attribution_basis
        , case service_or_team_attribution_basis
            when 'contact_additional_team' then c.uniq_other_care_prof_team_local_id
            when 'contact_legacy_team' then c.uniq_care_prof_team_id
            when 'referral_primary_team' then r.uniq_care_prof_team_local_id
        end as service_or_team_id
        , case service_or_team_attribution_basis
            when 'contact_additional_team' then c.other_care_prof_team_local_id
            when 'contact_legacy_team' then c.care_prof_team_local_id
            when 'referral_primary_team' then r.care_prof_team_local_id
        end as service_or_team_local_id
        , iff(service_or_team_attribution_basis = 'referral_primary_team',
            r.serv_team_type, null) as primary_team_type_code
        , iff(service_or_team_attribution_basis = 'referral_primary_team',
            r.service_type_name, null) as primary_team_type_name
        , iff(service_or_team_attribution_basis = 'referral_primary_team',
            r.serv_team_int_age_group, null) as primary_team_intended_age_group_code
    from {{ ref('stg_mhsds_carecontact') }} as c
    left join {{ ref('stg_mhsds_referral_history') }} as r
        on c.uniq_submission_id = r.uniq_submission_id
        and c.uniq_serv_req_id = r.uniq_serv_req_id
    left join {{ ref('stg_mhsds_activesubmission') }} as s
        on c.uniq_submission_id = s.uniq_submission_id
)
select
    c.mhs201_uniq_id
    , c.same_submission_referral_record_id
    , c.same_submission_referral_person_id
    , c.same_submission_referral_received_date
    , c.is_same_submission_referral_linked
    , c.is_same_submission_referral_person_consistent
    , c.service_or_team_attribution_basis
    , c.service_or_team_id
    , c.service_or_team_local_id
    , coalesce(c.primary_team_type_code, t.serv_team_type_mh) as service_or_team_type_code
    , coalesce(c.primary_team_type_name, t.service_type_name) as source_service_or_team_type_name
    , coalesce(c.primary_team_intended_age_group_code, t.serv_team_int_age_group)
        as service_or_team_intended_age_group_code
from contact_context as c
left join {{ ref('stg_mhsds_service_or_team_details') }} as t
    on c.uniq_submission_id = t.uniq_submission_id
    and c.service_or_team_local_id = t.care_prof_team_local_id
