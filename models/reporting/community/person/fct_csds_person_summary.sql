with population as (
    select
        person_id
        , min(first_reporting_period_end_date) as first_evidence_reporting_period_end_date
        , max(last_reporting_period_end_date) as last_evidence_reporting_period_end_date
        , count(*) as n_evidence_types
    from {{ ref('int_csds_person_evidence') }}
    group by person_id
)

-- Newest period record across providers.
, demographics as (
    select *
    from {{ ref('dim_csds_person_provider_period') }}
    qualify row_number() over (
        partition by person_id
        order by reporting_period_end_date desc nulls last, source_file_received_at desc nulls last,
            submission_id desc, source_row_id desc
    ) = 1
)

, referrals as (
    select
        person_id
        , count(*) as n_recorded_referrals
        , count_if(is_recorded_open) as n_open_referrals
        , min(referral_received_date) as first_referral_date
        , max(referral_received_date) as latest_referral_date
    from {{ ref('fct_csds_referral_summary') }}
    where person_id is not null
    group by person_id
)

, caseload as (
    select
        person_id
        , count(*) as n_current_caseload_providers
        , sum(n_open_referrals) as n_current_caseload_referrals
        , sum(n_open_referrals_without_attendance) as n_current_caseload_referrals_without_attendance
        , max(evidence_date) as current_caseload_evidence_date
    from {{ ref('fct_csds_current_caseload_person') }}
    group by person_id
)

, contact_measures as (
    select
        c.person_id
        , count(*) as n_recorded_contacts
        , max(iff(c.care_contact_date <= d.as_of_date, c.care_contact_date, null)) as latest_contact_date
        , max(iff(c.care_contact_date <= d.as_of_date and c.is_attended, c.care_contact_date, null))
            as latest_attended_contact_date
        , count_if(c.is_attended
            and c.care_contact_date between dateadd(month, -12, d.as_of_date) and d.as_of_date) as n_attended_contacts_12m
        , count_if(c.is_dna
            and c.care_contact_date between dateadd(month, -12, d.as_of_date) and d.as_of_date) as n_dna_contacts_12m
        , count_if(c.is_cancelled
            and c.care_contact_date between dateadd(month, -12, d.as_of_date) and d.as_of_date) as n_cancelled_contacts_12m
        , count_if(c.care_contact_date > d.as_of_date) as n_contacts_after_as_of_date
    from {{ ref('fct_csds_care_contact') }} as c
    cross join {{ ref('int_csds_reporting_date') }} as d
    where c.person_id is not null
    group by c.person_id
)

, clinical as (
    select
        person_id
        , count(*) as n_clinical_records
        , count_if(clinical_record_type = 'immunisation') as n_immunisation_records
        , count_if(clinical_record_type in ('referral_assessment', 'activity_assessment')) as n_assessment_records
    from {{ ref('fct_csds_clinical_record') }}
    where person_id is not null
    group by person_id
)

select
    p.person_id
    , b.sk_patient_id
    , d.as_of_date
    , p.first_evidence_reporting_period_end_date
    , p.last_evidence_reporting_period_end_date
    , p.n_evidence_types
    , demo.reporting_period_end_date as demographics_reporting_period_end_date
    , demo.provider_organisation_code as demographics_provider_organisation_code
    , demo.source_age_at_period_end as source_age_at_demographics_period_end
    , demo.person_stated_gender_code
    , demo.person_stated_gender_name
    , demo.ethnicity_2001_code
    , demo.ethnicity_2001_description
    , demo.ethnicity_2001_broad_group
    , demo.residence_local_authority_code
    , demo.residence_local_authority_name
    , demo.is_wnl_resident
    , demo.residence_imd_2025_decile
    , demo.person_death_date
    , demo.is_looked_after_child
    , demo.has_safeguarding_vulnerability_factors
    , coalesce(r.n_recorded_referrals, 0) as n_recorded_referrals
    , coalesce(r.n_open_referrals, 0) as n_open_referrals
    , r.first_referral_date
    , r.latest_referral_date
    , coalesce(cl.n_current_caseload_providers, 0) as n_current_caseload_providers
    , coalesce(cl.n_current_caseload_referrals, 0) as n_current_caseload_referrals
    , coalesce(cl.n_current_caseload_referrals_without_attendance, 0)
        as n_current_caseload_referrals_without_attendance
    , coalesce(cl.n_current_caseload_referrals, 0) > 0 as has_current_recorded_caseload
    , coalesce(c.n_recorded_contacts, 0) as n_recorded_contacts
    , c.latest_contact_date
    , c.latest_attended_contact_date
    , coalesce(c.n_attended_contacts_12m, 0) as n_attended_contacts_12m
    , coalesce(c.n_dna_contacts_12m, 0) as n_dna_contacts_12m
    , coalesce(c.n_cancelled_contacts_12m, 0) as n_cancelled_contacts_12m
    , coalesce(c.n_contacts_after_as_of_date, 0) as n_contacts_after_as_of_date
    , coalesce(cr.n_clinical_records, 0) as n_clinical_records
    , coalesce(cr.n_immunisation_records, 0) as n_immunisation_records
    , coalesce(cr.n_assessment_records, 0) as n_assessment_records
from population as p
cross join {{ ref('int_csds_reporting_date') }} as d
left join {{ ref('stg_csds_bridging') }} as b on p.person_id = b.person_id
left join demographics as demo on p.person_id = demo.person_id
left join referrals as r on p.person_id = r.person_id
left join caseload as cl on p.person_id = cl.person_id
left join contact_measures as c on p.person_id = c.person_id
left join clinical as cr on p.person_id = cr.person_id
