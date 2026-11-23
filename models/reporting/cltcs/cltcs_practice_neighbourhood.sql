{{
    config(
        materialized='table',
        tags=['cltcs'])
}}

/*
C-LTCS practice to neighbourhood mapping, one row per practice.

Starts from the shared dim_practice_neighbourhood. C-LTCS assigns a few practices
to a different neighbourhood than the shared mapping; those practices are listed
in neighbourhood_overrides, and their neighbourhood name is looked up from
neighbourhood_wnl_active for the override code.
*/

with neighbourhood_overrides as (
    -- TODO: placeholder overrides; replace with the agreed C-LTCS reassignments
    select practice_code, neighbourhood_code
    from (values
         ('F85060', 'HGY02')
        , ('Y03135', 'HGY02')
        , ('F85019', 'HGY02')
        , ('Y02117', 'HGY02')
        , ('F85675', 'HGY01')
        , ('F85031', 'HGY02')
        , ('F85623', 'HGY02')
        , ('F83017', 'CAM01')
    ) as v (practice_code, neighbourhood_code)
)

select p.practice_code
    , p.practice_name
    , p.local_authority
    , coalesce(o.neighbourhood_code, p.neighbourhood_code) as neighbourhood_code
    , case when o.practice_code is not null then n.neighbourhood_name
        else p.neighbourhood_registered end as neighbourhood_registered
    , o.practice_code is not null as is_cltcs_neighbourhood_override
from {{ ref('dim_practice_neighbourhood') }} p
left join neighbourhood_overrides o
    on o.practice_code = p.practice_code
left join {{ ref('neighbourhood_wnl_active') }} n
    on n.neighbourhood_code = o.neighbourhood_code
