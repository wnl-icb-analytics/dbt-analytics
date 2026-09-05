with expected as (
    select 'MHS601' as source_table, count(*) as source_count
    from {{ ref('stg_mhsds_previous_diagnosis') }}
    union all
    select 'MHS603', count(*) from {{ ref('stg_mhsds_provisional_diagnosis') }}
    union all
    select 'MHS604', count(*) from {{ ref('stg_mhsds_primdiag') }}
    union all
    select 'MHS605', count(*) from {{ ref('stg_mhsds_secondary_diagnosis') }}
)

, represented as (
    select source_table, sum(accepted_source_record_count) as represented_count
    from {{ ref('int_mhsds_diagnosis') }}
    group by source_table
)

select
    coalesce(e.source_table, r.source_table) as source_table
    , coalesce(e.source_count, 0) as source_count
    , coalesce(r.represented_count, 0) as represented_count
from expected as e
full outer join represented as r on e.source_table = r.source_table
where coalesce(e.source_count, 0) <> coalesce(r.represented_count, 0)
