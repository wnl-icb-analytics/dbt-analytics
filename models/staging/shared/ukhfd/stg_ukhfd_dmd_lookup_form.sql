select
    cd::varchar as form_code,
    "DESC" as form_description,
    in_source_data = 1 as is_in_latest_source
from {{ ref('raw_ukhfd_dmd_lookup_form') }}
where is_latest = 1
qualify row_number() over (
    partition by cd
    order by is_latest desc, in_source_data desc, effective_from desc nulls last,
        import_date desc nulls last, created_date desc nulls last
) = 1
