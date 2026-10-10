-- Confirmed transfer chains, with each member's position: 0 for the original referral, then the transfer number.
with chain_members as (
    select
        original_referral_id
        , original_referral_id as referral_id
        , 0 as chain_position
    from {{ ref('int_iapt_referral_transfer') }}
    where is_confirmed_transfer
        and transfer_number = 1

    union all

    select
        original_referral_id
        , successor_referral_id
        , transfer_number
    from {{ ref('int_iapt_referral_transfer') }}
    where is_confirmed_transfer
)

, member_dates as (
    select
        m.original_referral_id
        , m.referral_id
        , m.chain_position
        , r.first_assessment_date
        , r.first_treatment_date
        , r.last_treatment_date
    from chain_members as m
    inner join {{ ref('fct_iapt_referral') }} as r
        on m.referral_id = r.referral_id
)

-- Care recorded under earlier provider codes of the same course. Predecessors stop being reported before
-- their successor starts, so their latest dates are known by every successor period.
, predecessor_dates as (
    select
        successor.referral_id
        , min(predecessor.first_assessment_date) as first_assessment_date
        , min(predecessor.first_treatment_date) as first_treatment_date
        , max(predecessor.last_treatment_date) as last_treatment_date
    from member_dates as successor
    inner join member_dates as predecessor
        on successor.original_referral_id = predecessor.original_referral_id
        and predecessor.chain_position < successor.chain_position
    group by successor.referral_id
)

, periods as (
    select
        r.*
        , p.referral_id is not null as is_transfer_successor
        -- NHS England derivations on this version cover care up to its reporting month.
        , least(
            coalesce(r.assessment_first_date, p.first_assessment_date)
            , coalesce(p.first_assessment_date, r.assessment_first_date)
        ) as pathway_first_assessment_date
        , least(
            coalesce(r.therapy_session_first_date, p.first_treatment_date)
            , coalesce(p.first_treatment_date, r.therapy_session_first_date)
        ) as pathway_first_treatment_date
        , greatest(
            coalesce(r.therapy_session_last_date, p.last_treatment_date)
            , coalesce(p.last_treatment_date, r.therapy_session_last_date)
        ) as pathway_last_treatment_date
    from {{ ref('stg_iapt_referral_history') }} as r
    left join predecessor_dates as p
        on r.referral_id = p.referral_id
)

, states as (
    select
        *
        , referral_request_received_date <= reporting_period_end_date
            and (serv_disch_date is null or serv_disch_date > reporting_period_end_date)
            as is_recorded_open_at_period_end
        , case
            when referral_request_received_date is null then 'referral_date_missing'
            when referral_request_received_date > reporting_period_end_date then 'not_started'
            when serv_disch_date <= reporting_period_end_date then 'ended'
            when pathway_first_treatment_date is not null then 'in_treatment'
            when pathway_first_assessment_date is not null then 'awaiting_treatment'
            else 'awaiting_assessment'
        end as waiting_state
    from periods
)

select
    {{ dbt_utils.generate_surrogate_key(['s.submission_id', 's.referral_id']) }} as referral_period_id
    , s.referral_id
    , s.source_row_id
    , s.person_id
    , b.sk_patient_id
    , s.provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , coalesce(
        {{ is_wnl_icb_code(['s.dm_icb_commissioner', 's.dm_sub_icb_commissioner', 's.org_id_comm']) }}, false
    ) as is_wnl_commissioner
    , s.dm_icb_commissioner as source_icb_commissioner_code
    , icb.organisation_name as source_icb_commissioner_name
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.referral_request_received_date as referral_received_date
    , s.serv_disch_date as referral_discharge_date
    , s.is_transfer_successor
    , s.pathway_first_assessment_date as first_assessment_date
    , s.pathway_first_treatment_date as first_treatment_date
    , s.pathway_last_treatment_date as last_treatment_date
    , s.is_recorded_open_at_period_end
    , s.waiting_state
    -- Waits at period end as in NHS England measures M030 and M039 to M045.
    , iff(
        s.waiting_state in ('awaiting_assessment', 'awaiting_treatment')
        , datediff(day, s.referral_request_received_date, s.reporting_period_end_date)
        , null
    ) as days_waiting_at_period_end
    -- Time since the last treatment for open referrals, as in M019 to M022.
    , iff(
        s.is_recorded_open_at_period_end
        , datediff(day, s.pathway_last_treatment_date, s.reporting_period_end_date)
        , null
    ) as days_since_last_treatment
    , s.use_pathway_flag as is_nhse_use_pathway
    , s.submission_id
    , s.source_file_received_at
    , s.file_type
    , s.dataset_version
from states as s
left join {{ ref('stg_iapt_bridging') }} as b
    on s.person_id = b.person_id
left join {{ ref('organisation') }} as provider
    on s.provider_organisation_code = provider.organisation_code
left join {{ ref('organisation') }} as icb
    on s.dm_icb_commissioner = icb.organisation_code
