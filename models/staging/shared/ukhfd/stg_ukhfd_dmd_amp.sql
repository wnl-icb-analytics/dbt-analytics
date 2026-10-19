select
    apid::varchar as apid,
    vpid::varchar as vpid,
    nm as amp_name,
    invalid = 1 as is_invalid,
    in_source_data = 1 as is_in_latest_source
from {{ ref('raw_ukhfd_dmd_amp') }}
where is_latest = 1
qualify row_number() over (
    partition by apid
    order by is_latest desc, in_source_data desc, effective_from desc nulls last,
        import_date desc nulls last, created_date desc nulls last
) = 1
