select
    vpid::varchar as vpid,
    isid::varchar as ingredient_substance_id,
    basis_strntcd::varchar as basis_strength_code,
    strnt_nmrtr_val as strength_numerator_value,
    strnt_nmrtr_uomcd::varchar as strength_numerator_unit_code,
    strnt_dnmtr_val as strength_denominator_value,
    strnt_dnmtr_uomcd::varchar as strength_denominator_unit_code,
    in_source_data = 1 as is_in_latest_source
from {{ ref('raw_ukhfd_dmd_vmp_ingredient') }}
where is_latest = 1
qualify row_number() over (
    partition by vpid, isid
    order by is_latest desc, in_source_data desc, effective_from desc nulls last,
        import_date desc nulls last, created_date desc nulls last
) = 1
