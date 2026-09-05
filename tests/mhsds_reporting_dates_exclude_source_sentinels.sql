-- Diagnosis dates are not listed: the currency diagnosis model still passes source
-- values through uncast, so the sentinel rule does not yet hold there.
with profile_sentinel_counts as (
    select
        count_if(first_referral_date < '1901-01-01'::date) as first_referral_date,
        count_if(latest_referral_date < '1901-01-01'::date) as latest_referral_date,
        count_if(latest_contact_date < '1901-01-01'::date) as latest_contact_date,
        count_if(latest_detention_start_date < '1901-01-01'::date)
            as latest_detention_start_date
    from {{ ref('dim_person_mh_profile') }}
)

-- one row per date column so a failure names the column holding the sentinel
, profile_sentinels as (
    select
        offending_column,
        sentinel_row_count
    from profile_sentinel_counts
    unpivot (
        sentinel_row_count for offending_column in (
            first_referral_date,
            latest_referral_date,
            latest_contact_date,
            latest_detention_start_date
        )
    )
    where sentinel_row_count > 0
)

select
    'care_contact' as model_name,
    null::varchar as offending_column,
    count(*) as sentinel_row_count
from {{ ref('fct_mhsds_care_contact') }}
where least_ignore_nulls(
    care_contact_date,
    earliest_reasonable_offer_date,
    earliest_clinically_appropriate_date,
    appointment_offer_date,
    appointment_booked_date,
    care_contact_cancellation_date,
    reporting_period_start_date,
    reporting_period_end_date
) < '1901-01-01'::date
having count(*) > 0

union all

select
    'referral' as model_name,
    null::varchar as offending_column,
    count(*) as sentinel_row_count
from {{ ref('fct_mhsds_referral') }}
where least_ignore_nulls(
    referral_received_date,
    decision_to_treat_date,
    discharge_plan_created_date,
    discharge_plan_last_updated_date,
    referral_rejection_date,
    referral_discharge_date,
    discharge_letter_issued_date,
    record_start_date,
    record_end_date,
    reporting_period_start_date,
    reporting_period_end_date
) < '1901-01-01'::date
having count(*) > 0

union all

select
    'hospital_provider_spell' as model_name,
    null::varchar as offending_column,
    count(*) as sentinel_row_count
from {{ ref('fct_mhsds_hospital_provider_spell') }}
where least_ignore_nulls(
    admission_date,
    decided_to_admit_date,
    estimated_discharge_date,
    planned_discharge_date,
    discharge_date,
    record_start_date,
    record_end_date,
    reporting_period_start_date,
    reporting_period_end_date
) < '1901-01-01'::date
having count(*) > 0

union all

select
    'ward_stay' as model_name,
    null::varchar as offending_column,
    count(*) as sentinel_row_count
from {{ ref('fct_mhsds_ward_stay') }}
where least_ignore_nulls(
    ward_stay_start_date,
    ward_stay_end_date,
    trial_leave_end_date,
    record_start_date,
    record_end_date,
    reporting_period_start_date,
    reporting_period_end_date
) < '1901-01-01'::date
having count(*) > 0

union all

select
    'person_mh_profile' as model_name,
    offending_column,
    sentinel_row_count
from profile_sentinels

union all

select
    'clinical_record' as model_name,
    'clinical_date' as offending_column,
    count(*) as sentinel_row_count
from {{ ref('fct_mhsds_clinical_record') }}
where clinical_date < '1901-01-01'::date
having count(*) > 0
