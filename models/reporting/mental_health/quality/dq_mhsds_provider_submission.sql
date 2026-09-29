with submissions as (
    select
        org_id_provider as provider_organisation_code
        , reporting_period_end_date
        , count(*) as n_accepted_submissions
        , max(reporting_period_end_date) over (partition by org_id_provider)
            as provider_latest_reporting_period_end_date
    from {{ ref('stg_mhsds_activesubmission') }}
    group by org_id_provider, reporting_period_end_date
)
, referrals as (
    select org_id_prov, reporting_period_end_date, count(*) as n_referral_source_records
    from {{ ref('stg_mhsds_referral_history') }}
    group by org_id_prov, reporting_period_end_date
)
, contacts as (
    select org_id_prov, reporting_period_end_date, count(*) as n_contact_source_records
    from {{ ref('stg_mhsds_carecontact') }}
    group by org_id_prov, reporting_period_end_date
)
, patients as (
    select org_id_prov, reporting_period_end_date, count(*) as n_patient_source_records
    from {{ ref('stg_mhsds_mpi_history') }}
    group by org_id_prov, reporting_period_end_date
)
select
    s.provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.reporting_period_end_date
    , s.n_accepted_submissions
    , coalesce(r.n_referral_source_records, 0) as n_referral_source_records
    , coalesce(c.n_contact_source_records, 0) as n_contact_source_records
    , coalesce(p.n_patient_source_records, 0) as n_patient_source_records
    , s.provider_latest_reporting_period_end_date
    , d.as_of_date as dataset_latest_reporting_period_end_date
    , datediff(month, s.provider_latest_reporting_period_end_date, d.as_of_date)
        as provider_months_behind_dataset
    , s.reporting_period_end_date = s.provider_latest_reporting_period_end_date
        as is_latest_provider_period
from submissions as s
cross join {{ ref('int_mhsds_reporting_date') }} as d
left join referrals as r
    on s.provider_organisation_code = r.org_id_prov
    and s.reporting_period_end_date = r.reporting_period_end_date
left join contacts as c
    on s.provider_organisation_code = c.org_id_prov
    and s.reporting_period_end_date = c.reporting_period_end_date
left join patients as p
    on s.provider_organisation_code = p.org_id_prov
    and s.reporting_period_end_date = p.reporting_period_end_date
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.provider_organisation_code) = upper(provider.organisation_code)
