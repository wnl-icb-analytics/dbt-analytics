{{ config(materialized='view') }}

with imd_ranking as (
    select 
        rank () over (partition by effective_snapshot_date order by imd_score) as imd_rank,
        practice_code,
        effective_snapshot_date,
        imd_score
    from {{ ref('int_imd_score_by_practice_national')}} 
)

select 
    imd.practice_code,
    effective_snapshot_date,
    case when effective_snapshot_date = max(effective_snapshot_date) over () then true else false end as is_latest,
    wnl.practice_code is not null as is_wnl_practice,
    wnl.sub_icb_code as wnl_sub_icb_code,
    ceil((imd_rank * 10) / count(1) over (partition by effective_snapshot_date)) as imd_decile,
    imd_rank,
    imd_score
from imd_ranking imd

left join {{ ref('practice_wnl_all') }} wnl
on imd.practice_code = wnl.practice_code 

order by effective_snapshot_date desc, imd_rank