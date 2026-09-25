{{ config(materialized='table') }}

-- Team and GP practice context for each latest referral/contact pair. The
-- delivering team is the one the contact names; the referral's team is a
-- fallback only when that team cannot be resolved.
with contact as (
    select
        unique_service_request_identifier
        , unique_care_contact_identifier
        , cyp201_unique_id
        , unique_submission_id
        , reporting_period_end_date::date as reporting_period_end_date
        , person_id
        , organisation_code_provider
        , care_contact_date
        , care_professional_team_local_identifier
        , unique_care_professional_team_local_identifier
    from {{ ref('stg_csds_care_contact') }}
)

-- CYP102 is keyed by referral and local team. The few repeated keys within one
-- submission carry identical content.
, team_row as (
    select
        unique_submission_id
        , unique_service_request_identifier
        , care_professional_team_local_identifier
        , reporting_period_end_date::date as reporting_period_end_date
        , service_or_team_type_referred_to_community_care as service_or_team_type_code
    from {{ ref('stg_csds_service_type_history') }}
    qualify row_number() over (
        partition by unique_submission_id, unique_service_request_identifier,
            care_professional_team_local_identifier
        order by cyp102_unique_id desc
    ) = 1
)

-- One version per referral, team and reporting period for the as-of lookup.
, team_by_period as (
    select *
    from team_row
    where care_professional_team_local_identifier is not null
    qualify row_number() over (
        partition by unique_service_request_identifier, care_professional_team_local_identifier,
            reporting_period_end_date
        order by unique_submission_id desc
    ) = 1
)

-- A team identifier is provider-level; use it across referrals in the same
-- submission only when every relationship gives it the same type.
, submission_team_type as (
    select
        unique_submission_id
        , care_professional_team_local_identifier
        , min(service_or_team_type_code) as service_or_team_type_code
    from team_row
    where care_professional_team_local_identifier is not null
    group by unique_submission_id, care_professional_team_local_identifier
    having count(distinct service_or_team_type_code) = 1
)

-- The referral's team type in the same submission, only when all its teams agree.
, submission_referral_type as (
    select
        unique_submission_id
        , unique_service_request_identifier
        , min(service_or_team_type_code) as service_or_team_type_code
    from team_row
    group by unique_submission_id, unique_service_request_identifier
    having count(distinct service_or_team_type_code) = 1
)

, same_submission as (
    select
        c.*
        , t.service_or_team_type_code as same_submission_type_code
    from contact as c
    left join team_row as t
        on c.unique_submission_id = t.unique_submission_id
        and c.unique_service_request_identifier = t.unique_service_request_identifier
        and c.care_professional_team_local_identifier = t.care_professional_team_local_identifier
)

-- The newest version of the same referral and team reported at or before the
-- contact's period, for contacts not resolved in their own submission.
, other_submission as (
    select
        c.unique_service_request_identifier
        , c.unique_care_contact_identifier
        , h.service_or_team_type_code as other_submission_type_code
    from (
        select * from same_submission
        where same_submission_type_code is null and care_professional_team_local_identifier is not null
    ) as c
    asof join team_by_period as h
        match_condition(c.reporting_period_end_date >= h.reporting_period_end_date)
        on c.unique_service_request_identifier = h.unique_service_request_identifier
        and c.care_professional_team_local_identifier = h.care_professional_team_local_identifier
)

, team as (
    select
        c.unique_service_request_identifier
        , c.unique_care_contact_identifier
        , case
            when c.same_submission_type_code is not null then 'contact_team'
            when o.other_submission_type_code is not null then 'contact_team_other_submission'
            when m.service_or_team_type_code is not null then 'contact_team_submission_map'
            when r.service_or_team_type_code is not null and c.care_professional_team_local_identifier is null
                then 'referral_team_contact_team_missing'
            when r.service_or_team_type_code is not null then 'referral_team_contact_team_unmatched'
            else 'unresolved'
        end as service_or_team_attribution_basis
        , coalesce(
            c.same_submission_type_code
            , o.other_submission_type_code
            , m.service_or_team_type_code
            , r.service_or_team_type_code
        ) as service_or_team_type_code
    from same_submission as c
    left join other_submission as o
        on c.unique_service_request_identifier = o.unique_service_request_identifier
        and c.unique_care_contact_identifier = o.unique_care_contact_identifier
    left join submission_team_type as m
        on c.unique_submission_id = m.unique_submission_id
        and c.care_professional_team_local_identifier = m.care_professional_team_local_identifier
    left join submission_referral_type as r
        on c.unique_submission_id = r.unique_submission_id
        and c.unique_service_request_identifier = r.unique_service_request_identifier
)

-- Several providers can report the same registration period; the newest report wins.
, practice_at_contact as (
    select
        c.unique_service_request_identifier
        , c.unique_care_contact_identifier
        , gp.general_medical_practice_code_patient_registration as practice_code
    from contact as c
    inner join {{ ref('stg_csds_gp_registration') }} as gp
        on c.person_id = gp.person_id
        and c.care_contact_date >= gp.start_date_gmp_patient_registration
        and (
            gp.end_date_gmp_patient_registration is null
            or c.care_contact_date < gp.end_date_gmp_patient_registration
        )
    qualify row_number() over (
        partition by c.unique_service_request_identifier, c.unique_care_contact_identifier
        order by gp.start_date_gmp_patient_registration desc nulls last, gp.reporting_period_end_date desc nulls last,
            gp.effective_from desc nulls last, gp.unique_submission_id desc, gp.cyp002_unique_id desc
    ) = 1
)

, latest_gp_registration as (
    select
        person_id
        , general_medical_practice_code_patient_registration as practice_code
    from {{ ref('stg_csds_gp_registration') }}
    qualify row_number() over (
        partition by person_id
        order by start_date_gmp_patient_registration desc nulls last, reporting_period_end_date desc nulls last,
            effective_from desc nulls last, unique_submission_id desc, cyp002_unique_id desc
    ) = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['c.unique_service_request_identifier', 'c.unique_care_contact_identifier']) }}
        as care_contact_source_record_id
    , c.unique_service_request_identifier
    , c.unique_care_contact_identifier
    , c.cyp201_unique_id
    , c.unique_submission_id
    , c.care_professional_team_local_identifier as service_or_team_local_id
    , c.unique_care_professional_team_local_identifier as service_or_team_id
    , t.service_or_team_type_code
    , t.service_or_team_attribution_basis
    , coalesce(at_contact.practice_code, latest.practice_code) as practice_code
    , case
        when at_contact.practice_code is not null then 'at_contact'
        when latest.practice_code is not null then 'latest_known'
    end as practice_attribution
from contact as c
inner join team as t
    on c.unique_service_request_identifier = t.unique_service_request_identifier
    and c.unique_care_contact_identifier = t.unique_care_contact_identifier
left join practice_at_contact as at_contact
    on c.unique_service_request_identifier = at_contact.unique_service_request_identifier
    and c.unique_care_contact_identifier = at_contact.unique_care_contact_identifier
left join latest_gp_registration as latest
    on c.person_id = latest.person_id
