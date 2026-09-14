select
    trim(organisation_code)::varchar as provider_code,
    trim(organisation_name)::varchar as provider_name,
    mff_value::number(28, 18) as mff_factor,
    mff_value_year_2::number(28, 18) as mff_factor_year_2,
    mff_value_year_3::number(28, 18) as mff_factor_year_3,
    mff_value_year_4::number(28, 18) as mff_factor_year_4,
    mff_value_year_5::number(28, 18) as mff_factor_year_5,
    notes,
    file_name as source_file_name,
    import_date::timestamp_ntz as source_imported_at,
    created_date::date as source_created_date
from {{ ref('raw_ukhfd_market_forces_factor_values') }}
