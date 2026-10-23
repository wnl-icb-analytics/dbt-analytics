{{ config(materialized='table') }}

-- CKD_62 helper: one row per eligible, registered, living person aged at least 17.
-- EMIS XML requires at least two positive UACR records, then tests the latest
-- positive result and the latest positive result on an earlier calendar date > 4.
-- There is no recency window or minimum interval beyond the earlier date.
-- Used before screening exclusions and by the later CKD helper exclusions.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_61_base') }}
),

-- Equivalent local laboratory roots also permit mapped concepts and requested descendants.
positive_uacr as (
    select o.person_id, o.observation_id, o.clinical_effective_date, o.date_recorded, o.result_value, o.valueset_id
    from ({{ get_ltc_lcs_observations("c75746ce-5d7b-1647-f08a-ef5b10445f4c", match_paths='both', include_date_recorded=true) }}) as o
    inner join base as b on o.person_id = b.person_id
    where o.result_value > 0
),

repeated_uacr as (
    select person_id
    from positive_uacr
    where valueset_id = 'c75746ce-5d7b-1647-f08a-ef5b10445f4c'
    group by person_id
    having count(distinct observation_id) >= 2
),

latest_uacr as (
    select person_id, clinical_effective_date, result_value
    from positive_uacr
    where valueset_id = 'c75746ce-5d7b-1647-f08a-ef5b10445f4c'
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc nulls last, date_recorded desc nulls last, observation_id desc
    ) = 1
),

-- Rank within the earlier-date pool before testing the retained result.
earlier_uacr as (
    select child.person_id, child.result_value
    from positive_uacr as child
    inner join latest_uacr as parent on child.person_id = parent.person_id
        and child.clinical_effective_date::date < parent.clinical_effective_date::date
    where child.valueset_id = 'c75746ce-5d7b-1647-f08a-ef5b10445f4c'
    qualify row_number() over (
        partition by child.person_id
        order by child.clinical_effective_date desc nulls last, child.date_recorded desc nulls last, child.observation_id desc
    ) = 1
)

select latest.person_id
from latest_uacr as latest
inner join repeated_uacr as repeated on latest.person_id = repeated.person_id
inner join earlier_uacr as earlier on latest.person_id = earlier.person_id
where latest.result_value > 4
    and earlier.result_value > 4
    and not exists (
        select 1 from {{ ref('int_ltc_lcs_cf_ckd_61_woEX') }} as excluded
        where excluded.person_id = latest.person_id
    )
