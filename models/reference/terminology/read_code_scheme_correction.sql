with dictionary as (
    select 'read_v2' as coding_system, read_code, read_code_alt
    from {{ ref('stg_dictionary_dbo_readcodes') }}
    where is_read_v2
    union all
    select 'ctv3', read_code, read_code_alt
    from {{ ref('stg_dictionary_dbo_readcodes') }}
    where is_ctv3
)

-- Every code read_code could match, taken before its ambiguity filter: an ambiguous
-- alternative is still evidence that the code belongs to its own scheme.
, membership as (
    select coding_system, read_code as code from dictionary
    union all
    select coding_system, read_code_alt from dictionary where read_code_alt is not null
    union all
    select 'read_v2', code from {{ ref('stg_ukhfd_read_v2_term') }}
    union all
    select 'read_v2', read_code from {{ ref('stg_ukhfd_read_v2_term') }}
    union all
    select 'ctv3', code from {{ ref('stg_ukhfd_ctv3_concept') }}
    union all
    select 'ctv3', code from {{ ref('stg_ukhfd_ctv3_term') }}
)

, concepts as (
    select distinct coding_system, read_code as code
    from dictionary
)

select
    iff(c.coding_system = 'read_v2', 'ctv3', 'read_v2') as submitted_coding_system
    , c.code
    , c.coding_system as resolved_coding_system
from concepts as c
where not exists (
    select 1
    from membership as m
    where m.code = c.code
        and m.coding_system <> c.coding_system
)
