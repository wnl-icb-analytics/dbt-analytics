select *
from {{ ref("stg_dictionary_dbo_organisation") }}
-- ECDS provider identifiers include NHS provider/trust records, current
-- trust records held under the generic type, and independent UEC providers.
where sk_organisation_type_id in (0, 41, 54)
qualify row_number() over (
    partition by organisation_code order by coalesce(last_updated, first_created) desc
) = 1
