select
    geography_code as code,
    geography_name as name,
    introduced_date,
    terminated_date
from {{ ref('stg_ukhfd_ons_codes_equivalents') }}
qualify row_number() over (
    partition by geography_code
    order by
        introduced_date desc nulls last,
        register_year desc nulls last,
        source_imported_at desc nulls last,
        source_created_at desc nulls last,
        -- Retain a recorded termination when source revisions tie or are undated.
        terminated_date desc nulls last,
        geography_name asc nulls last
) = 1
