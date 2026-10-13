{{ config(materialized = 'view') }}

select
    sk_organisation_id
    , geom
    , date_created
    , date_updated
from {{ ref('raw_dictionary_geo_os_ccg_map') }}
where sk_organisation_id is not null
  and geom is not null
