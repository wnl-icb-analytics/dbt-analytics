select
    r.*
    , p.dataset_latest_reporting_period_end_date
    , p.provider_months_behind_dataset
    , datediff(day, r.referral_received_date, r.reporting_period_end_date) as days_since_referral_received
    , datediff(day, r.last_recorded_attended_contact_date, r.reporting_period_end_date)
        as days_since_last_attended_contact
    , case
        when r.first_recorded_attended_contact_date is null then 'no_attended_contact_recorded'
        when r.last_recorded_attended_contact_date >= r.reporting_period_start_date then 'attended_in_latest_provider_period'
        else 'attendance_recorded_in_earlier_period'
    end as recorded_caseload_contact_state
from {{ ref('fct_csds_referral_period') }} as r
inner join {{ ref('dq_csds_provider_submission') }} as p
    on r.provider_organisation_code = p.provider_organisation_code
    and r.reporting_period_end_date = p.reporting_period_end_date
    and p.is_latest_provider_period
where r.is_recorded_open_at_period_end
