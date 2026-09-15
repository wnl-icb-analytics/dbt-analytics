-- A contact is accepted only in its own month's file (ETOS v2.1.22 IDS201 row 6), so a contact,
-- activity or IDS607 identifier reused in another month is another record. Every accepted history
-- key, qualified by reporting month, must reach its fact once. Returns aggregate counts only.
with checks as (
    select
        'care_contact' as record_type
        , (
            select count(*) from (
                select distinct referral_id, care_contact_id, unique_month_id
                from {{ ref('stg_iapt_care_contact_history') }}
            ) as contact_keys
        ) as history_keys
        , (select count(*) from {{ ref('fct_iapt_care_contact') }}) as fact_rows

    union all

    select
        'care_activity'
        , (
            select count(*) from (
                select distinct referral_id, care_contact_id, unique_month_id, care_activity_id
                from {{ ref('stg_iapt_care_activity_history') }}
            ) as activity_keys
        )
        , (select count(*) from {{ ref('fct_iapt_care_activity') }})

    union all

    select
        'activity_assessment'
        , (
            select count(*) from (
                select distinct referral_id, care_contact_id, unique_month_id, care_activity_id, coded_ass_tool_type
                from {{ ref('stg_iapt_activity_assessment_history') }}
            ) as assessment_keys
        )
        , (select count(*) from {{ ref('fct_iapt_assessment_score') }} where source_table = 'IDS607')
)

select record_type, history_keys, fact_rows
from checks
where history_keys <> fact_rows
