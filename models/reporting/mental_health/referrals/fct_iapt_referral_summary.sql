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

-- Course dates across a confirmed transfer chain. The predecessor's contacts precede the successor's, so the
-- chain's second treatment is the second earliest of its members' first and second treatment dates.
, chain_treatment_dates as (
    select original_referral_id, first_treatment_date as treatment_date
    from referrals
    where first_treatment_date is not null
    union all
    select original_referral_id, second_treatment_date
    from referrals
    where second_treatment_date is not null
)

, chain_second_treatment as (
    select
        original_referral_id
        , (array_agg(treatment_date) within group (order by treatment_date))[1]::date
            as pathway_second_treatment_date
    from chain_treatment_dates
    group by original_referral_id
)

, pathway as (
    select
        r.*
        -- An inferred end closes the timeline only; it is never a discharge date.
        , case r.as_of_referral_status
            when 'discharged' then r.service_discharge_date
            when 'no_longer_submitted' then r.last_reported_period_end_date
            when 'transferred' then r.last_reported_period_end_date
        end as referral_end_date
        , case r.as_of_referral_status
            when 'discharged' then 'discharged'
            when 'open' then 'open'
            else 'last_submission'
        end as referral_end_date_source
        -- NHS England derives course fields per provider-qualified referral, so a successor's cover only the
        -- part after the transfer.
        , r.is_transfer_successor as has_partial_nhse_course_fields
        , min(r.first_assessment_date) over (partition by r.original_referral_id) as pathway_first_assessment_date
        , min(r.first_treatment_date) over (partition by r.original_referral_id) as pathway_first_treatment_date
        , c.pathway_second_treatment_date
    from referrals as r
    left join chain_second_treatment as c
        on r.original_referral_id = c.original_referral_id
)

select
    *
    -- NHS England ended referral measures M073 to M076, on this referral's own supplied counts and flag.
    , case
        when service_discharge_date is null then null
        when is_completed_treatment then 'finished_course_treatment'
        when nhse_treatment_contact_count = 1 then 'treated_once'
        when nhse_treatment_contact_count = 0 and nhse_care_contact_count > 0 then 'seen_not_treated'
        when nhse_care_contact_count = 0 then 'not_seen'
    end as ended_referral_type
    -- M193 numerator over the M195 denominator: finished a course of treatment, not recorded as below caseness
    -- at the start. Null outside that denominator.
    , case
        when is_completed_treatment is distinct from true then null
        when caseness_at_start_status = 'not_at_caseness' then null
        else coalesce(is_recovered and reliable_change_status = 'reliable_improvement', false)
    end as is_reliably_recovered
    -- Waits as in M024 to M037 and M046: calendar days from referral receipt. Chain dates keep a transfer
    -- successor's wait from the original receipt; members of a chain share the received date.
    , datediff(day, referral_received_date, pathway_first_assessment_date) as days_referral_to_first_assessment
    , datediff(day, referral_received_date, pathway_first_treatment_date) as days_referral_to_first_treatment
    , days_referral_to_first_treatment <= 42 as is_first_treatment_within_6_weeks
    , days_referral_to_first_treatment <= 126 as is_first_treatment_within_18_weeks
    , datediff(day, pathway_first_treatment_date, pathway_second_treatment_date) as days_first_to_second_treatment
from pathway
