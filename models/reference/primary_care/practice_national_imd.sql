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
    practice_code,
    effective_snapshot_date,
    ceil((imd_rank * 10) / count(1) over (partition by effective_snapshot_date)) as imd_decile,
    imd_rank,
    imd_score
from imd_ranking

order by effective_snapshot_date desc, imd_rank