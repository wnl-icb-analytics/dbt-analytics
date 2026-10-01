with source as (
    select cast(practice_code as varchar) as practice_code
        , cast(neighbourhood_code as varchar) as neighbourhood_code
        , cast(practice_name as varchar) as practice_name
        , cast(neighbourhood_registered as varchar) as neighbourhood_registered
        , cast(local_authority as varchar) as local_authority
    from {{ref('raw_cltcs_emis_cltcs_local_mapping_nh_gp')}}
),

flagged as (
    select
        practice_code
        , neighbourhood_code
        , practice_name
        , neighbourhood_registered
        , local_authority
        , case
            when lower(trim(local_authority)) = lower(regexp_substr(trim(neighbourhood_registered), '[^ ]+$'))
            then 1 else 0
          end as la_is_last_word_flag
    from source
)

select
    practice_code
    , neighbourhood_code
    , practice_name
    , neighbourhood_registered
    , local_authority
from flagged
-- Prefer rows where the local_authority is the last word of neighbourhood_registered.
-- Where a practice has at least one such row, keep only those; otherwise keep ALL of
-- that practice's rows (no deterministic single-row tiebreak -- both are kept to trigger an error).
qualify la_is_last_word_flag = max(la_is_last_word_flag) over (partition by practice_code)
