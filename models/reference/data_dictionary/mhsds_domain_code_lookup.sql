select *
from {{ ref('stg_ukhfd_data_dictionary_mhsds_domain_code_lookup') }}
qualify row_number() over (
    partition by code_set_name, code
    order by is_latest_definition desc, source_effective_from_at desc nulls last,
        source_imported_at desc nulls last, source_unique_key desc, source_reference_model
) = 1
