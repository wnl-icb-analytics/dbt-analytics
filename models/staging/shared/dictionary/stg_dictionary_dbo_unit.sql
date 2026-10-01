select
    sk_unit_id
    , nullif(trim(unit_symbol), '') as unit_symbol
    , nullif(trim(unit_name), '') as unit_name
    , nullif(trim(quantity_name), '') as quantity_name
    , si_power
    , is_standard_unit
    , is_derived_unit
from {{ ref('raw_dictionary_dbo_unit') }}
