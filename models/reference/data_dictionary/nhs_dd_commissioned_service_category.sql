--additional column for funding status to be used in the activity vs plan model
select
    commissioned_service_category_code as code
    , commissioned_service_category_description as description
    -- Drops the trailing lower-case usage note, e.g. "(funded nationally)"; keeps "(NOT LISTED)"
    , trim(regexp_replace(commissioned_service_category_description, '\\s*\\([a-z].*\\)\\s*$', '')) as short_description
    , is_in_latest_source as is_currently_valid
    , source_snapshot_date
    ,CASE commissioned_service_category_code
         WHEN 12 THEN 'ICB-commissioned'
         WHEN 21 THEN 'Retained'
         WHEN 22 THEN 'Retained'
         WHEN 25 THEN 'Delegated'
         WHEN 26 THEN 'Delegated'
         WHEN 31 THEN 'Retained'
         WHEN 32 THEN 'Retained'
         WHEN 41 THEN 'Retained'
         WHEN 51 THEN 'Retained'
         WHEN 55 THEN 'Delegated'
         WHEN 61 THEN 'Retained'
         WHEN 71 THEN 'Retained'
         WHEN 75 THEN 'Delegated'
         WHEN 81 THEN 'Retained'
         WHEN 85 THEN 'Delegated'
         WHEN 91 THEN 'Retained'
         WHEN 92 THEN 'Retained'
         WHEN 93 THEN 'Retained'
         WHEN 98 THEN 'Other/Unknown'
         WHEN 99 THEN 'Other/Unknown'
         END AS funding_status
from {{ ref('stg_ukhfd_dsc_sc_commissioned_service_category') }}
where is_latest_definition
