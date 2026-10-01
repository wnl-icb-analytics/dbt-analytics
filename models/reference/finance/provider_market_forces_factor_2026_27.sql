select
    provider_code::varchar(10) as provider_code,
    provider_name,
    mff_factor::float as mff_factor,
    source_file_name,
    source_imported_at
from {{ ref('stg_ukhfd_market_forces_factor_values') }}
where source_created_date = '2026-04-01'
  and endswith(source_file_name, '26-27-NHSPS-Annex-A-Prices-workbook.xlsx')
