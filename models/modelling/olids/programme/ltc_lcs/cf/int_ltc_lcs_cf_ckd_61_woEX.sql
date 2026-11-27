{{ config(materialized='table') }}

-- CKD_61 helper: one row per eligible, registered, living person aged at least 17.
-- EMIS XML requires at least two positive eGFR records, then tests the latest
-- positive result and the latest positive result on an earlier calendar date < 60.
-- There is no recency window or minimum interval beyond the earlier date.
-- Used before screening exclusions and by the later CKD helper exclusions.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_61_base') }}
),

-- Equivalent local laboratory roots also permit mapped concepts and requested descendants.
positive_egfr as (
    select o.person_id, o.observation_id, o.clinical_effective_date, o.date_recorded, o.result_value, o.valueset_id
    from ({{ get_ltc_lcs_observations("cc380a59-cc4c-ee42-4a2e-30ab9286dc71,c9896ca5-15c8-0009-a57a-bd6f5067dcad", match_paths='both', include_date_recorded=true, exclude_result_headers=true) }}) as o
    inner join base as b on o.person_id = b.person_id
    where o.result_value > 0
),

repeated_egfr as (
    select person_id
    from positive_egfr
    where valueset_id = 'cc380a59-cc4c-ee42-4a2e-30ab9286dc71'
    group by person_id
    having count(distinct observation_id) >= 2
),

latest_egfr as (
    select person_id, clinical_effective_date, result_value
    from positive_egfr
    where valueset_id = 'c9896ca5-15c8-0009-a57a-bd6f5067dcad'
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc nulls last, date_recorded desc nulls last, observation_id desc
    ) = 1
),

-- Rank within the earlier-date pool before testing the retained result.
earlier_egfr as (
    select child.person_id, child.result_value
    from positive_egfr as child
    inner join latest_egfr as parent on child.person_id = parent.person_id
        and child.clinical_effective_date::date < parent.clinical_effective_date::date
    where child.valueset_id = 'c9896ca5-15c8-0009-a57a-bd6f5067dcad'
    qualify row_number() over (
        partition by child.person_id
        order by child.clinical_effective_date desc nulls last, child.date_recorded desc nulls last, child.observation_id desc
    ) = 1
)

select latest.person_id
from latest_egfr as latest
inner join repeated_egfr as repeated on latest.person_id = repeated.person_id
inner join earlier_egfr as earlier on latest.person_id = earlier.person_id
where latest.result_value < 60
    and earlier.result_value < 60
