-- UKHFD holds one year's delegation status, so per-year statuses come from the NHSE workbook seed
with service_code as (
    select *
    from {{ ref('stg_ukhfd_dsc_sc_service_code') }}
    where is_latest_definition
),

delegation as (
    select
        service_code
        , icb_delegation_24_25
        , icb_delegation_25_26
        , icb_delegation_26_27
    from {{ ref('nhse_service_codes_by_year_v38') }}
)

select
    sc.service_code
    , sc.service_code_description
    , sc.service_code_including_description
    , sc.service_category_description
    , sc.npoc_category_and_crg
    , sc.highly_specialised_service
    , sc.service_grouping
    , sc.notes
    , sc.effective_from_date
    , sc.effective_to_date
    , sc.is_current
    , d.icb_delegation_24_25
    , d.icb_delegation_25_26
    , d.icb_delegation_26_27
    , sc.source_snapshot_date
from service_code as sc
left join delegation as d
    on sc.service_code = d.service_code
