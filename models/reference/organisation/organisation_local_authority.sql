select *
from {{ ref("stg_dictionary_dbo_organisation") }}
where organisation_primary_role = 'RO141'
qualify row_number() over (
    partition by organisation_code order by coalesce(last_updated, first_created) desc
) = 1