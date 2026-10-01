select *
from {{ ref("stg_dictionary_dbo_organisation") }}
-- ECDS attendance-site fields contain hospital sites, UTCs, walk-in centres,
-- independent sites and legacy/unspecified site records.
where sk_organisation_type_id in (0, 38, 42, 55)
qualify row_number() over (
    partition by organisation_code order by coalesce(last_updated, first_created) desc
) = 1
