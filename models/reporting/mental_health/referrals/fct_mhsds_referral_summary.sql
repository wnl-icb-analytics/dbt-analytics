with contacts as (
    select
        referral_source_record_id
        , count(*) as n_contacts
        , count_if(is_attended) as n_attended_contacts
        , count_if(is_dna) as n_dna_contacts
        , count_if(is_cancelled) as n_cancelled_contacts
        , count_if(attendance_status_code is null) as n_contacts_with_missing_attendance_status
        , count_if(is_latest_referral_person_consistent = false)
            as n_contacts_with_different_referral_person
        , count_if(is_latest_referral_person_consistent is null)
            as n_contacts_without_comparable_referral_person
        , min(care_contact_date) as first_contact_date
        , max(care_contact_date) as latest_contact_date
        , min(iff(is_attended, care_contact_date, null))
            as first_attended_contact_date
    from {{ ref('fct_mhsds_care_contact') }}
    group by referral_source_record_id
)
, indirect_activity as (
    select uniq_serv_req_id, count(*) as n_indirect_activity_records
    from {{ ref('stg_mhsds_indirectactivity') }}
    group by uniq_serv_req_id
)
, spells as (
    select referral_source_record_id, count(*) as n_recorded_hospital_spells
    from {{ ref('fct_mhsds_hospital_provider_spell') }}
    group by referral_source_record_id
)
, teams as (
    select referral_source_record_id
        , count(*) as n_service_team_relationships
        , count(distinct service_or_team_id) as n_recorded_service_teams
    from {{ ref('rel_mhsds_referral_service_team') }}
    group by referral_source_record_id
)
select
    r.*
    , d.as_of_date
    , coalesce(c.n_contacts, 0) as n_contacts
    , coalesce(c.n_attended_contacts, 0) as n_attended_contacts
    , coalesce(c.n_dna_contacts, 0) as n_dna_contacts
    , coalesce(c.n_cancelled_contacts, 0) as n_cancelled_contacts
    , coalesce(c.n_contacts_with_missing_attendance_status, 0) as n_contacts_with_missing_attendance_status
    , coalesce(c.n_contacts_with_different_referral_person, 0)
        as n_contacts_with_different_referral_person
    , coalesce(c.n_contacts_without_comparable_referral_person, 0)
        as n_contacts_without_comparable_referral_person
    , coalesce(c.n_contacts_with_different_referral_person > 0, false)
        as has_contact_referral_person_disagreement
    , c.first_contact_date
    , c.latest_contact_date
    , c.first_attended_contact_date
    , iff(c.first_attended_contact_date < r.referral_received_date, null,
        datediff(day, r.referral_received_date, c.first_attended_contact_date))
        as days_to_first_attended_contact
    , coalesce(c.first_attended_contact_date < r.referral_received_date, false)
        as has_pre_referral_attended_contact
    , coalesce(i.n_indirect_activity_records, 0) as n_indirect_activity_records
    , coalesce(s.n_recorded_hospital_spells, 0) as n_recorded_hospital_spells
    , coalesce(t.n_service_team_relationships, 0) as n_service_team_relationships
    , coalesce(t.n_recorded_service_teams, 0) as n_recorded_service_teams
    , iff(r.referral_status = 'open' and r.referral_received_date <= d.as_of_date
        and (c.first_attended_contact_date is null or c.first_attended_contact_date > d.as_of_date),
        datediff(day, r.referral_received_date, d.as_of_date), null)
        as days_since_referral_without_attended_contact
from {{ ref('fct_mhsds_referral') }} as r
cross join {{ ref('int_mhsds_reporting_date') }} as d
left join contacts as c on r.source_record_id = c.referral_source_record_id
left join indirect_activity as i on r.source_record_id = i.uniq_serv_req_id
left join spells as s on r.source_record_id = s.referral_source_record_id
left join teams as t on r.source_record_id = t.referral_source_record_id
