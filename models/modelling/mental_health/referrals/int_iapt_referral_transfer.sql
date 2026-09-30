-- Referral keys are provider-qualified. When a provider's ODS code changes, its open referrals are resubmitted
-- under the new code with new local and pathway identifiers and none of their earlier contacts.
with referrals as (
    select
        referral_id
        , person_id
        , referral_received_date
        , provider_organisation_code
        , service_discharge_date
        , first_reported_period_end_date
        , last_reported_period_end_date
    from {{ ref('fct_iapt_referral') }}
    where person_id is not null
        and referral_received_date is not null
)

-- The successor first appears in the month after the predecessor's last month. Wider gaps add almost no
-- matches and link the first copy of a twice-transferred referral to its third.
, transfers as (
    select
        successor.referral_id as successor_referral_id
        , predecessor.referral_id as predecessor_referral_id
        , successor.provider_organisation_code as successor_provider_organisation_code
        , predecessor.provider_organisation_code as predecessor_provider_organisation_code
        , successor.first_reported_period_end_date as successor_first_reported_period_end_date
        , predecessor.last_reported_period_end_date as predecessor_last_reported_period_end_date
    from referrals as predecessor
    inner join referrals as successor
        on predecessor.person_id = successor.person_id
        and predecessor.referral_received_date = successor.referral_received_date
        and predecessor.provider_organisation_code <> successor.provider_organisation_code
        and datediff(
            month, predecessor.last_reported_period_end_date, successor.first_reported_period_end_date
        ) = 1
    where predecessor.service_discharge_date is null
    -- A referral with more than one candidate on either side is left unlinked.
    qualify count(*) over (partition by predecessor.referral_id) = 1
        and count(*) over (partition by successor.referral_id) = 1
)

-- Walk each chain from its first referral; a referral can transfer more than once.
, chains as (
    select
        t.successor_referral_id
        , t.predecessor_referral_id as original_referral_id
        , 1 as transfer_number
    from transfers as t
    left join transfers as earlier
        on t.predecessor_referral_id = earlier.successor_referral_id
    where earlier.successor_referral_id is null

    union all

    select
        t.successor_referral_id
        , c.original_referral_id
        , c.transfer_number + 1
    from transfers as t
    inner join chains as c
        on t.predecessor_referral_id = c.successor_referral_id
)

select
    t.successor_referral_id
    , t.predecessor_referral_id
    , c.original_referral_id
    , c.transfer_number
    , t.successor_provider_organisation_code
    , t.predecessor_provider_organisation_code
    , t.successor_first_reported_period_end_date
    , t.predecessor_last_reported_period_end_date
from transfers as t
inner join chains as c
    on t.successor_referral_id = c.successor_referral_id
