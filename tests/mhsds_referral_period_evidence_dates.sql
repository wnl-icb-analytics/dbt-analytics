select
    count_if(first_recorded_attended_contact_date > reporting_period_end_date
        or last_recorded_attended_contact_date > reporting_period_end_date
        or attendance_evidence_available_by_date > reporting_period_end_date) as future_attendance_records
    , count_if(is_recorded_open_at_period_end and (
        referral_received_date > reporting_period_end_date
        or referral_rejection_date <= reporting_period_end_date
        or referral_discharge_date <= reporting_period_end_date)) as invalid_open_records
from {{ ref('fct_mhsds_referral_period') }}
having future_attendance_records > 0 or invalid_open_records > 0
