{{ config(materialized="table") }}

with latest_submission_periods as (
    select distinct
        la_code,
        reporting_period_start_date,
        reporting_period_end_date
    from {{ ref("stg_asc_cld_adult_social_care_cld") }}
    qualify row_number() over (
        partition by la_code
        order by reporting_period_end_date desc, reporting_period_start_date desc
    ) = 1
),

borough_names as(
    select organisation_code, organisation_name 
    from {{ ref("organisation_local_authority") }} 
),

latest_period_cld as (
    select borough.organisation_name as borough_name, asc_cld.*
    from {{ ref("stg_asc_cld_adult_social_care_cld") }} as asc_cld
    inner join latest_submission_periods as latest
        on asc_cld.la_code = latest.la_code
        and asc_cld.reporting_period_start_date = latest.reporting_period_start_date
        and asc_cld.reporting_period_end_date = latest.reporting_period_end_date
    left join borough_names as borough
        on asc_cld.la_code = borough.organisation_code
    where asc_cld.sk_patient_id is not null 
    and latest.reporting_period_end_date between dateadd(month, -18, current_date()) and current_date() -- reporting period is relatively up to date at time of review [this is more for records with errors in LA code so they aren't too old] TO DO: consider requirement for valid borough id
),

-- use_within_scope as ( -- not adding a date filter as reporting period is up to date at time of review
--     select * from latest_period_cld
--     where event_start_date between dateadd(month, -24, current_date()) and current_date()
-- )

recommended_dedupe as ( -- https://www.ardengemcsu.nhs.uk/media/3748/cld-dashboard-methodology-document-for-users.pdf
    select
        sk_patient_id,
        la_code,
        borough_name,
        reporting_period_start_date,
        reporting_period_end_date,
        event_start_date,
        client_type,
        primary_support_reason,
        client_funding_status,
        service_type,
        service_component,
        delivery_mechanism
    from latest_period_cld
    where event_type = 'Service'
    qualify row_number() over (
        partition by
            la_code,
            sk_patient_id,
            event_start_date,
            service_type,
            service_component,
            delivery_mechanism
        order by dm_received_date desc, dm_row_id desc
    ) = 1
),

cleaned_support_reasons as (
    select
        sk_patient_id,
        la_code,
        borough_name,
        reporting_period_start_date,
        reporting_period_end_date,
        service_type,
        {{ clean_asc_cld_primary_support_reason('primary_support_reason') }}
            as primary_support_reason_cleaned
        , primary_support_reason
    from recommended_dedupe
),

categorised_support_reasons as (
    select
        sk_patient_id,
        la_code,
        borough_name,
        reporting_period_start_date,
        reporting_period_end_date,
        service_type,
        primary_support_reason_cleaned,
        {{ asc_cld_primary_support_reason_category('primary_support_reason_cleaned') }}
            as primary_support_reason_category
        , primary_support_reason
    from cleaned_support_reasons
)

select * from categorised_support_reasons

