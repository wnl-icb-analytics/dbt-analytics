with accepted as (
    select
        a.submission_id
        , a.provider_organisation_code
        , a.reporting_period_end_date
        , a.file_type
        , a.dataset_version
        , a.source_file_received_at
        , a.source_loaded_at
        , max(a.reporting_period_end_date) over (partition by a.provider_organisation_code)
            as provider_latest_reporting_period_end_date
        , max(a.reporting_period_end_date) over () as dataset_latest_reporting_period_end_date
        , h.file_total_ids101
        , h.file_total_ids201
        , h.file_total_ids202
        , h.file_total_ids607
    from {{ ref('stg_iapt_activesubmission') }} as a
    left join {{ ref('stg_iapt_submission_header') }} as h
        on a.submission_id = h.submission_id
    where a.provider_organisation_code is not null
        and a.reporting_period_end_date is not null
)

-- The Primary file a Refresh replaced; the latest received when a provider sent more than one.
, superseded_primary as (
    select
        h.provider_organisation_code
        , h.reporting_period_end_date
        , h.file_total_ids101
        , h.file_total_ids201
        , h.file_total_ids202
        , h.file_total_ids607
    from {{ ref('stg_iapt_submission_header') }} as h
    left join {{ ref('stg_iapt_activesubmission') }} as a
        on h.submission_id = a.submission_id
    where a.submission_id is null
        and h.file_type = 'Primary'
    qualify row_number() over (
        partition by h.provider_organisation_code, h.reporting_period_end_date
        order by h.source_file_received_at desc nulls last, h.submission_id desc
    ) = 1
)

, referrals as (
    select
        submission_id
        , count(*) as n_referral_source_records
        , count_if(referral_request_received_date between reporting_period_start_date and reporting_period_end_date)
            as n_new_referrals
        -- The data set version names the referral source field, as in fct_iapt_referral.
        , count_if(
            referral_request_received_date between reporting_period_start_date and reporting_period_end_date
            and iff(dataset_version = '2.0', source_of_referral_mh, source_of_referral_iapt) is null
        ) as n_new_referrals_missing_source
        , count_if(serv_disch_date between reporting_period_start_date and reporting_period_end_date)
            as n_discharges
        , count_if(serv_disch_date between reporting_period_start_date and reporting_period_end_date
            and end_code is null) as n_discharges_missing_reason
    from {{ ref('stg_iapt_referral_history') }}
    group by submission_id
)

, contacts as (
    select
        submission_id
        , count(*) as n_contact_source_records
        , count_if(cons_mechanism is null and cons_medium_used is null) as n_contacts_missing_mechanism
    from {{ ref('stg_iapt_care_contact_history') }}
    group by submission_id
)

, activities as (
    select submission_id, count(*) as n_activity_source_records
    from {{ ref('stg_iapt_care_activity_history') }}
    group by submission_id
)

, activity_assessments as (
    select submission_id, count(*) as n_activity_assessment_source_records
    from {{ ref('stg_iapt_activity_assessment_history') }}
    group by submission_id
)

, measured as (
    select
        a.*
        , coalesce(r.n_referral_source_records, 0) as n_referral_source_records
        , coalesce(c.n_contact_source_records, 0) as n_contact_source_records
        , coalesce(ac.n_activity_source_records, 0) as n_activity_source_records
        , coalesce(aa.n_activity_assessment_source_records, 0) as n_activity_assessment_source_records
        , coalesce(r.n_new_referrals, 0) as n_new_referrals
        , coalesce(r.n_discharges, 0) as n_discharges
        , r.n_new_referrals_missing_source / nullif(r.n_new_referrals, 0)
            as new_referrals_missing_source_of_referral_ratio
        , r.n_discharges_missing_reason / nullif(r.n_discharges, 0) as discharges_missing_reason_ratio
        , c.n_contacts_missing_mechanism / nullif(c.n_contact_source_records, 0)
            as contacts_missing_consultation_mechanism_ratio
        , p.file_total_ids101 as primary_file_total_ids101
        , p.file_total_ids201 as primary_file_total_ids201
        , p.file_total_ids202 as primary_file_total_ids202
        , p.file_total_ids607 as primary_file_total_ids607
    from accepted as a
    left join referrals as r
        on a.submission_id = r.submission_id
    left join contacts as c
        on a.submission_id = c.submission_id
    left join activities as ac
        on a.submission_id = ac.submission_id
    left join activity_assessments as aa
        on a.submission_id = aa.submission_id
    left join superseded_primary as p
        on a.file_type = 'Refresh'
        and a.provider_organisation_code = p.provider_organisation_code
        and a.reporting_period_end_date = p.reporting_period_end_date
)

select
    m.provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , m.reporting_period_end_date
    , m.submission_id
    , m.file_type
    , m.dataset_version
    , m.source_file_received_at
    , m.source_loaded_at
    , datediff(day, m.reporting_period_end_date, m.source_file_received_at) as days_period_end_to_file_received
    , datediff(day, m.source_file_received_at, m.source_loaded_at) as days_file_received_to_loaded
    , m.n_referral_source_records
    , m.n_contact_source_records
    , m.n_activity_source_records
    , m.n_activity_assessment_source_records
    , m.file_total_ids101
    , m.file_total_ids201
    , m.file_total_ids202
    , m.file_total_ids607
    , m.primary_file_total_ids101
    , m.primary_file_total_ids201
    , m.primary_file_total_ids202
    , m.primary_file_total_ids607
    , m.file_total_ids101 / nullif(m.primary_file_total_ids101, 0) as referral_refresh_to_primary_ratio
    , m.file_total_ids201 / nullif(m.primary_file_total_ids201, 0) as contact_refresh_to_primary_ratio
    , m.file_total_ids202 / nullif(m.primary_file_total_ids202, 0) as activity_refresh_to_primary_ratio
    , m.file_total_ids607 / nullif(m.primary_file_total_ids607, 0) as activity_assessment_refresh_to_primary_ratio
    -- A refresh restates the whole month. Under half the rows of the Primary it replaced, in a section where
    -- the Primary held at least 100, points to a truncated file rather than ordinary correction.
    , case
        when m.primary_file_total_ids101 is null then null
        else (m.primary_file_total_ids101 >= 100 and m.file_total_ids101 < 0.5 * m.primary_file_total_ids101)
            or (m.primary_file_total_ids201 >= 100 and m.file_total_ids201 < 0.5 * m.primary_file_total_ids201)
            or (m.primary_file_total_ids202 >= 100 and m.file_total_ids202 < 0.5 * m.primary_file_total_ids202)
            or (m.primary_file_total_ids607 >= 100 and m.file_total_ids607 < 0.5 * m.primary_file_total_ids607)
    end as has_refresh_section_shortfall
    , m.n_new_referrals
    , m.new_referrals_missing_source_of_referral_ratio
    , m.n_discharges
    , m.discharges_missing_reason_ratio
    , m.contacts_missing_consultation_mechanism_ratio
    -- Flagged when at least half of at least 20 eligible rows lack the item.
    , m.n_new_referrals >= 20 and m.new_referrals_missing_source_of_referral_ratio >= 0.5
        as is_source_of_referral_mostly_missing
    , m.n_discharges >= 20 and m.discharges_missing_reason_ratio >= 0.5 as is_discharge_reason_mostly_missing
    , m.n_contact_source_records >= 20 and m.contacts_missing_consultation_mechanism_ratio >= 0.5
        as is_consultation_mechanism_mostly_missing
    , m.provider_latest_reporting_period_end_date
    , m.dataset_latest_reporting_period_end_date
    , datediff(month, m.provider_latest_reporting_period_end_date, m.dataset_latest_reporting_period_end_date)
        as provider_months_behind_dataset
    , m.reporting_period_end_date = m.provider_latest_reporting_period_end_date as is_latest_provider_period
from measured as m
left join {{ ref('organisation') }} as provider
    on m.provider_organisation_code = provider.organisation_code
