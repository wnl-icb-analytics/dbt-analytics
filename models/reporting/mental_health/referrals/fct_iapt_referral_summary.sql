-- Latest accepted month in the data set. Not the build date, and not every provider has submitted it.
with reporting_date as (
    select max(reporting_period_end_date) as as_of_date
    from {{ ref('stg_iapt_activesubmission') }}
    where reporting_period_end_date is not null
        and provider_organisation_code is not null
)

, provider_periods as (
    select
        provider_organisation_code
        , max(reporting_period_end_date) as provider_latest_reporting_period_end_date
    from {{ ref('stg_iapt_activesubmission') }}
    where reporting_period_end_date is not null
        and provider_organisation_code is not null
    group by provider_organisation_code
)

, referrals as (
    select
        r.*
        , d.as_of_date
        , pp.provider_latest_reporting_period_end_date
        , datediff(month, pp.provider_latest_reporting_period_end_date, d.as_of_date) as provider_months_behind_dataset
        , r.reporting_period_end_date = pp.provider_latest_reporting_period_end_date as is_in_latest_provider_period
        , ts.predecessor_referral_id
        , tp.successor_referral_id
        , ts.successor_referral_id is not null as is_transfer_successor
        , tp.predecessor_referral_id is not null as is_transfer_predecessor
        , coalesce(ts.original_referral_id, r.referral_id) as original_referral_id
        -- Open referrals must be resubmitted every month (user guidance v1.6.1 pp15 and 32). Without a discharge
        -- date, a referral last reported within 2 months of as_of_date is open; an older one ended at its last
        -- reported month, as in int_mhsds_spell_encounters. A referral continued under another provider code
        -- is transferred, not ended.
        , case
            when r.service_discharge_date is not null then 'discharged'
            when tp.predecessor_referral_id is not null then 'transferred'
            when date_trunc('month', r.last_reported_period_end_date)
                >= dateadd(month, -2, date_trunc('month', d.as_of_date)) then 'open'
            else 'no_longer_submitted'
        end as as_of_referral_status
    from {{ ref('fct_iapt_referral') }} as r
    cross join reporting_date as d
    left join provider_periods as pp
        on r.provider_organisation_code = pp.provider_organisation_code
    -- The transfer model is unique on both keys, so neither join adds rows. Unconfirmed candidates are ignored.
    left join {{ ref('int_iapt_referral_transfer') }} as ts
        on r.referral_id = ts.successor_referral_id
        and ts.is_confirmed_transfer
    left join {{ ref('int_iapt_referral_transfer') }} as tp
        on r.referral_id = tp.predecessor_referral_id
        and tp.is_confirmed_transfer
)

select
    *
    -- An inferred end closes the timeline only; it is never a discharge date.
    , case as_of_referral_status
        when 'discharged' then service_discharge_date
        when 'no_longer_submitted' then last_reported_period_end_date
        when 'transferred' then last_reported_period_end_date
    end as referral_end_date
    , case as_of_referral_status
        when 'discharged' then 'discharged'
        when 'open' then 'open'
        else 'last_submission'
    end as referral_end_date_source
    -- NHS England derives course fields per provider-qualified referral, so a successor's cover only the
    -- part after the transfer.
    , is_transfer_successor as has_partial_nhse_course_fields
    , min(first_assessment_date) over (partition by original_referral_id) as pathway_first_assessment_date
    , min(first_treatment_date) over (partition by original_referral_id) as pathway_first_treatment_date
from referrals
