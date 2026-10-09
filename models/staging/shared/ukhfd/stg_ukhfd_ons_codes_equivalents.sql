select
    geography_code
    , geography_name
    , geography_name_welsh
    , entity_code
    , status as geography_code_status
    , year::int as register_year
    , date_of_introduction as introduced_date
    , date_of_termination as terminated_date
    , ons_geography_code
    , ons_geography_name
    , dclg_geography_code
    , dclg_geography_name
    , dh_geography_code
    , dh_geography_name
    , scottish_geography_code
    , scottish_geography_name
    , ni_geography_code
    , ni_geography_name
    , wg_geography_code
    , wg_geography_name
    , wg_geography_name_welsh
    , import_date as source_imported_at
    , created_date as source_created_at
from {{ ref('raw_ukhfd_ons_codes_equivalents') }}
