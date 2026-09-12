-- NHS England dates activities and their assessments from the contact in the same
-- submission (DARS Table Consolidation Rules rows 30 and 37). Time is inherited only
-- when that contact records the same referral and person.
with activities as (
    select
        submission_id
        , care_activity_id
        , care_contact_id
        , referral_id
        , person_id
        , dmic_activity_date
    from {{ ref('stg_iapt_care_activity_history') }}
)

, contacts as (
    select
        submission_id
        , care_contact_id
        , referral_id
        , person_id
        , care_cont_date
        , care_cont_time
    from {{ ref('stg_iapt_care_contact_history') }}
)

, linked as (
    select
        a.submission_id
        , a.care_activity_id
        , a.care_contact_id
        , a.referral_id
        , a.person_id
        , a.dmic_activity_date as source_derived_date
        , c.care_contact_id is not null as is_submitted_contact_linked
        , iff(c.care_contact_id is null, null, a.referral_id is not distinct from c.referral_id)
            as is_submitted_contact_referral_consistent
        , iff(c.care_contact_id is null, null, a.person_id is not distinct from c.person_id)
            as is_submitted_contact_person_consistent
        , case
            when c.care_contact_id is null then 'parent_not_linked'
            when not (is_submitted_contact_referral_consistent and is_submitted_contact_person_consistent)
                then 'parent_inconsistent'
            when c.care_cont_date is null then 'missing'
            -- Project source-epoch rule: earlier dates are placeholders, not contact dates.
            when c.care_cont_date < '1901-01-01'::date then 'source_sentinel'
            else 'recorded'
        end as clinical_date_status
        , iff(clinical_date_status = 'recorded', c.care_cont_date, null) as clinical_date
        , iff(clinical_date_status = 'recorded', c.care_cont_time, null) as clinical_time
    from activities as a
    left join contacts as c
        on a.submission_id = c.submission_id
        and a.care_contact_id = c.care_contact_id
)

select
    *
    -- Midnight is a sort anchor for date-only rows, not an observed time.
    , case
        when clinical_date is null then null
        when clinical_time is null then clinical_date::timestamp_ntz
        else timestamp_ntz_from_parts(clinical_date, clinical_time)
    end as clinical_at
    , case
        when clinical_date is null then 'unknown'
        when clinical_time is null then 'date'
        else 'timestamp'
    end as clinical_time_precision
    , iff(clinical_date is null, 'not_recorded', 'same_submission_care_contact') as clinical_time_basis
    , iff(clinical_date is null or source_derived_date is null, null, clinical_date <> source_derived_date)
        as is_source_date_inconsistent
from linked
