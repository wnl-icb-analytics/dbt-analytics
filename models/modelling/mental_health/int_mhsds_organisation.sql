with organisations as (
    select
        upper(organisation_code) as organisation_code
        , organisation_name
        , organisation_primary_role
    from {{ ref('stg_dictionary_dbo_organisation') }}
    where organisation_code is not null
    qualify row_number() over (
        partition by upper(organisation_code)
        order by coalesce(last_updated, first_created) desc
    ) = 1
)

select
    o.organisation_code
    , o.organisation_name
    , o.organisation_primary_role
    -- RO261 is the ODS primary role for integrated care boards.
    , o.organisation_primary_role = 'RO261' as is_integrated_care_board
    , wnl.commissioner_code is not null as is_wnl_commissioner
from organisations as o
left join {{ ref('wnl_commissioner_icb_lookup') }} as wnl
    on o.organisation_code = upper(wnl.commissioner_code)
