{{ config(materialized='table', tags=['cltcs']) }}

-- Person-level summary of cltcs_gap_items: one row per person in the C-LTCS population with
-- a column per item, per domain group and per domain. The item and group column lists come
-- from the cltcs_gap_item_ids and cltcs_gap_domains macros.
-- Weight on gap depth in each domain gap score: a group with a gap scores 1 + this weight x
-- the share of its items that are gaps. Keep below 1 so breadth across conditions outranks
-- depth within one.
{% set gap_depth_weight = 0.5 %}
{% set gap_item_ids = cltcs_gap_item_ids() %}
{% set gap_domains = cltcs_gap_domains() %}

with spine_persons as (
    select distinct person_id
    from {{ ref('cltcs_adult_population') }}
    where meets_original_criteria or meets_adults_with_complexity
),

gap_items as (
    select person_id
        , item_id
        , score_domain
        , score_group
        , is_gap
    from {{ ref('cltcs_gap_items') }}
),

-- each group with a gap among its assessable items adds 1 for breadth, plus
-- gap_depth_weight x the share of its items that are gaps for depth. With the weight below 1
-- a group adds less than 2, so gaps across more conditions always outrank more gaps within
-- one condition (multimorbidity dominates), and a condition with many indicators cannot
-- outweigh one with few.
group_gaps as (
    select person_id
        , score_domain
        , score_group
        , boolor_agg(is_gap) as has_gap
        , count_if(is_gap) as items_with_gap
        , count(*) as items_assessed
        , max(iff(is_gap, 1, 0)) + {{ gap_depth_weight }} * avg(iff(is_gap, 1, 0)) as group_gap_score
    from gap_items
    group by person_id, score_domain, score_group
),

item_columns as (
    select person_id
        {%- for item_id in gap_item_ids %}
        , boolor_agg(iff(item_id = '{{ item_id }}', is_gap, null)) as is_gap_{{ item_id | lower }}
        {%- endfor %}
    from gap_items
    group by person_id
),

group_columns as (
    select person_id
        {%- for d in gap_domains %}
        {%- for group in d.groups %}
        , boolor_agg(iff(score_domain = '{{ d.domain }}' and score_group = '{{ group }}', has_gap, null)) as has_gap_{{ d.stem }}_{{ group }}
        {%- endfor %}
        , count_if(score_domain = '{{ d.domain }}' and has_gap) as {{ d.stem }}_groups_with_gap
        , count_if(score_domain = '{{ d.domain }}') as {{ d.stem }}_groups_assessed
        , sum(iff(score_domain = '{{ d.domain }}', items_with_gap, 0)) as {{ d.stem }}_items_with_gap
        , sum(iff(score_domain = '{{ d.domain }}', items_assessed, 0)) as {{ d.stem }}_items_assessed
        , sum(iff(score_domain = '{{ d.domain }}', group_gap_score, 0)) as {{ d.stem }}_gap_score
        {%- endfor %}
    from group_gaps
    group by person_id
)

select sp.person_id
    {%- for d in gap_domains %}
    -- {{ d.domain }}
    , zeroifnull(gc.{{ d.stem }}_groups_with_gap) as {{ d.stem }}_groups_with_gap
    , zeroifnull(gc.{{ d.stem }}_groups_assessed) as {{ d.stem }}_groups_assessed
    , zeroifnull(gc.{{ d.stem }}_items_with_gap) as {{ d.stem }}_items_with_gap
    , zeroifnull(gc.{{ d.stem }}_items_assessed) as {{ d.stem }}_items_assessed
    , zeroifnull(gc.{{ d.stem }}_gap_score) as {{ d.stem }}_gap_score
    {%- for group in d.groups %}
    , gc.has_gap_{{ d.stem }}_{{ group }}
    {%- endfor %}
    {%- endfor %}
    {%- for item_id in gap_item_ids %}
    , ic.is_gap_{{ item_id | lower }}
    {%- endfor %}
from spine_persons sp
left join item_columns ic
    on sp.person_id = ic.person_id
left join group_columns gc
    on sp.person_id = gc.person_id
