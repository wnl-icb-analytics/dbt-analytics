with activity as (
    select
        provider_organisation_code
        , service_or_team_id
        , service_or_team_local_id
        , service_or_team_type_code
        , date_trunc(month, care_contact_date)::date as activity_month
        , count(*) as n_contacts
        , count_if(service_or_team_attribution_basis = 'referral_primary_team')
            as n_contacts_attributed_to_referral_primary_team
        , count_if(service_or_team_attribution_basis = 'unresolved')
            as n_contacts_without_resolved_team
        , count_if(service_or_team_type_code is null) as n_contacts_without_team_type
        , count(distinct person_id) as n_people_with_recorded_identity
        , count_if(person_id is null) as n_contacts_without_person
        , count_if(is_attended) as n_attended_contacts
        , count_if(is_dna) as n_dna_contacts
        , count_if(is_cancelled) as n_cancelled_contacts
        , count_if(attendance_status_code is null) as n_contacts_with_missing_attendance
        , sum(iff(is_attended, clinical_contact_duration_minutes, null))
            as recorded_attended_duration_minutes
        , count_if(is_attended and clinical_contact_duration_minutes is null)
            as n_attended_contacts_with_missing_duration
    from {{ ref('fct_mhsds_care_contact') }}
    group by 1, 2, 3, 4, 5
)
select
    {{ dbt_utils.generate_surrogate_key([
        'a.provider_organisation_code', 'a.service_or_team_id',
        'a.service_or_team_local_id', 'a.service_or_team_type_code', 'a.activity_month'
    ]) }} as service_activity_month_id
    , a.provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , a.service_or_team_id
    , a.service_or_team_local_id
    , a.service_or_team_type_code
    , team.description as service_or_team_type_description
    , a.activity_month
    , a.n_contacts
    , a.n_contacts_attributed_to_referral_primary_team
    , a.n_contacts_without_resolved_team
    , a.n_contacts_without_team_type
    , a.n_people_with_recorded_identity
    , a.n_contacts_without_person
    , a.n_attended_contacts
    , a.n_dna_contacts
    , a.n_cancelled_contacts
    , a.n_contacts_with_missing_attendance
    , a.recorded_attended_duration_minutes
    , a.n_attended_contacts_with_missing_duration
from activity as a
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(a.provider_organisation_code) = upper(provider.organisation_code)
left join {{ ref('mhsds_service_or_team_type') }} as team
    on a.service_or_team_type_code = team.code
