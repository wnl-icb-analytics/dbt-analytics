select
    vpid::varchar as vpid,
    vtmid::varchar as vtm_id,
    nm as vmp_name,
    invalid = 1 as is_invalid,
    combprodcd::varchar as combination_product_code,
    udfs as unit_dose_form_size,
    udfs_uomcd::varchar as unit_dose_form_size_unit_code,
    in_source_data = 1 as is_in_latest_source
from {{ ref('raw_ukhfd_dmd_vmp') }}
where is_latest = 1
qualify row_number() over (
    partition by vpid
    order by is_latest desc, in_source_data desc, effective_from desc nulls last,
        import_date desc nulls last, created_date desc nulls last
) = 1
