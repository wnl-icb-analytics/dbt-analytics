select
    nullif(upper(trim(organisation_code)), '') as organisation_code
    , nullif(trim(organisation_name), '') as organisation_name
    , open_date::date as organisation_open_date
    , close_date::date as organisation_close_date
    , effective_from as source_effective_from_at
from {{ ref('raw_ukhfd_closed_organisations') }}
where nullif(trim(organisation_code), '') is not null
-- The archive can contain several names for a code at the same revision time.
qualify row_number() over (
    partition by upper(trim(organisation_code))
    order by effective_from desc nulls last
        , close_date desc nulls last
        , open_date desc nulls last
        , import_date desc nulls last
        , created_date desc nulls last
        , organisation_name
) = 1
