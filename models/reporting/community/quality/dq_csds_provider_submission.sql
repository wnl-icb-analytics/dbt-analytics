with submissions as (
    select
        provider_organisation_code
        , reporting_period_end_date
        , count(*) as n_accepted_submissions
        , max(reporting_period_end_date) over (partition by provider_organisation_code)
            as provider_latest_reporting_period_end_date
    from {{ ref('stg_csds_activesubmission') }}
    group by provider_organisation_code, reporting_period_end_date
)

, referrals as (
    select organisation_code_provider, reporting_period_end_date::date as reporting_period_end_date
        , count(*) as n_referral_source_records
    from {{ ref('stg_csds_referral_history') }}
    group by 1, 2
)

, contacts as (
    select organisation_code_provider, reporting_period_end_date::date as reporting_period_end_date
        , count(*) as n_contact_source_records
    from {{ ref('stg_csds_care_contact_history') }}
    group by 1, 2
)

, teams as (
    select organisation_code_provider, reporting_period_end_date::date as reporting_period_end_date
        , count(*) as n_team_source_records
    from {{ ref('stg_csds_service_type_history') }}
    group by 1, 2
)

, patients as (
    select organisation_code_provider, reporting_period_end_date::date as reporting_period_end_date
        , count(*) as n_patient_source_records
    from {{ ref('stg_csds_mpi_history') }}
    group by 1, 2
)

, counts as (
    select
        s.provider_organisation_code
        , s.reporting_period_end_date
        , s.n_accepted_submissions
        , s.provider_latest_reporting_period_end_date
        , coalesce(r.n_referral_source_records, 0) as n_referral_source_records
        , coalesce(c.n_contact_source_records, 0) as n_contact_source_records
        , coalesce(t.n_team_source_records, 0) as n_team_source_records
        , coalesce(p.n_patient_source_records, 0) as n_patient_source_records
    from submissions as s
    left join referrals as r
        on s.provider_organisation_code = r.organisation_code_provider
        and s.reporting_period_end_date = r.reporting_period_end_date
    left join contacts as c
        on s.provider_organisation_code = c.organisation_code_provider
        and s.reporting_period_end_date = c.reporting_period_end_date
    left join teams as t
        on s.provider_organisation_code = t.organisation_code_provider
        and s.reporting_period_end_date = t.reporting_period_end_date
    left join patients as p
        on s.provider_organisation_code = p.organisation_code_provider
        and s.reporting_period_end_date = p.reporting_period_end_date
)

select
    n.provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , n.reporting_period_end_date
    , n.n_accepted_submissions
    , n.n_referral_source_records
    , n.n_contact_source_records
    , n.n_team_source_records
    , n.n_patient_source_records
    -- Month-on-month ratios expose partial files and step changes.
    , n.n_referral_source_records / nullif(lag(n.n_referral_source_records) over (
        partition by n.provider_organisation_code order by n.reporting_period_end_date), 0)
        as referral_records_ratio_to_previous_period
    , n.n_contact_source_records / nullif(lag(n.n_contact_source_records) over (
        partition by n.provider_organisation_code order by n.reporting_period_end_date), 0)
        as contact_records_ratio_to_previous_period
    , n.n_team_source_records / nullif(lag(n.n_team_source_records) over (
        partition by n.provider_organisation_code order by n.reporting_period_end_date), 0)
        as team_records_ratio_to_previous_period
    , n.n_referral_source_records = 0 or n.n_contact_source_records = 0 or n.n_team_source_records = 0
        as has_missing_section
    , n.provider_latest_reporting_period_end_date
    , d.as_of_date as dataset_latest_reporting_period_end_date
    , datediff(month, n.provider_latest_reporting_period_end_date, d.as_of_date)
        as provider_months_behind_dataset
    , n.reporting_period_end_date = n.provider_latest_reporting_period_end_date
        as is_latest_provider_period
from counts as n
cross join {{ ref('int_csds_reporting_date') }} as d
left join {{ ref('organisation') }} as provider
    on upper(trim(n.provider_organisation_code)) = provider.organisation_code
