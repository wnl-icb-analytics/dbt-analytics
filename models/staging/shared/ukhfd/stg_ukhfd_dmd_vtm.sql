select
    vtmid::varchar as vtm_id,
    nm as vtm_name,
    vtmidprev::varchar as previous_vtm_id,
    in_source_data = 1 as is_in_latest_source
from {{ ref('raw_ukhfd_dmd_vtm') }}
where is_latest = 1
qualify row_number() over (
    partition by vtmid
    order by is_latest desc, in_source_data desc, effective_from desc nulls last,
        import_date desc nulls last, created_date desc nulls last
) = 1
