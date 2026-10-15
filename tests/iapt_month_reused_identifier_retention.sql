-- A contact is accepted only in its own month's file (ETOS v2.1.22 IDS201 row 6), so a contact,
-- activity or IDS607 identifier reused in another month is another record. Every accepted history
-- key, qualified by reporting month, must reach its fact once. Equal totals can hide a missing
-- key offset by an extra row, so this compares membership and fact cardinality. Returns
-- aggregate counts only.
with contact_history as (
    select referral_id, care_contact_id, unique_month_id, 1 as in_history
    from {{ ref('stg_iapt_care_contact_history') }}
    group by referral_id, care_contact_id, unique_month_id
)

, contact_fact as (
    select referral_id, care_contact_id, unique_month_id, count(*) as fact_rows
    from {{ ref('fct_iapt_care_contact') }}
    group by referral_id, care_contact_id, unique_month_id
)

, activity_history as (
    select referral_id, care_contact_id, unique_month_id, care_activity_id, 1 as in_history
    from {{ ref('stg_iapt_care_activity_history') }}
    group by referral_id, care_contact_id, unique_month_id, care_activity_id
)

, activity_fact as (
    select referral_id, care_contact_id, unique_month_id, care_activity_id, count(*) as fact_rows
    from {{ ref('fct_iapt_care_activity') }}
    group by referral_id, care_contact_id, unique_month_id, care_activity_id
)

, assessment_history as (
    select referral_id, care_contact_id, unique_month_id, care_activity_id, coded_ass_tool_type, 1 as in_history
    from {{ ref('stg_iapt_activity_assessment_history') }}
    group by referral_id, care_contact_id, unique_month_id, care_activity_id, coded_ass_tool_type
)

, assessment_fact as (
    select referral_id, care_contact_id, unique_month_id, care_activity_id, assessment_tool_code, count(*) as fact_rows
    from {{ ref('fct_iapt_assessment_score') }}
    where source_table = 'IDS607'
    group by referral_id, care_contact_id, unique_month_id, care_activity_id, assessment_tool_code
)

, comparisons as (
    select
        'care_contact' as record_type
        , count_if(f.fact_rows is null) as missing_from_fact
        , count_if(h.in_history is null) as unexpected_in_fact
        , count_if(f.fact_rows is not null and f.fact_rows <> 1) as fact_key_not_unique
    from contact_history as h
    full outer join contact_fact as f
        on h.referral_id is not distinct from f.referral_id
        and h.care_contact_id is not distinct from f.care_contact_id
        and h.unique_month_id is not distinct from f.unique_month_id

    union all

    select
        'care_activity'
        , count_if(f.fact_rows is null)
        , count_if(h.in_history is null)
        , count_if(f.fact_rows is not null and f.fact_rows <> 1)
    from activity_history as h
    full outer join activity_fact as f
        on h.referral_id is not distinct from f.referral_id
        and h.care_contact_id is not distinct from f.care_contact_id
        and h.unique_month_id is not distinct from f.unique_month_id
        and h.care_activity_id is not distinct from f.care_activity_id

    union all

    select
        'activity_assessment'
        , count_if(f.fact_rows is null)
        , count_if(h.in_history is null)
        , count_if(f.fact_rows is not null and f.fact_rows <> 1)
    from assessment_history as h
    full outer join assessment_fact as f
        on h.referral_id is not distinct from f.referral_id
        and h.care_contact_id is not distinct from f.care_contact_id
        and h.unique_month_id is not distinct from f.unique_month_id
        and h.care_activity_id is not distinct from f.care_activity_id
        and h.coded_ass_tool_type is not distinct from f.assessment_tool_code
)

, failures as (
    select record_type, 'missing_from_fact' as violation_type, missing_from_fact as failing_count
    from comparisons
    union all
    select record_type, 'unexpected_in_fact', unexpected_in_fact
    from comparisons
    union all
    select record_type, 'fact_key_not_unique', fact_key_not_unique
    from comparisons
)

select record_type, violation_type, failing_count
from failures
where failing_count > 0
