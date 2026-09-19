select
    trim(organisationcode_practice) as practice_code,
    trim(organisationcode_commissioner) as commissioner_code,
    cast(relationshipstartdate as date) as effective_from,
    cast(relationshipenddate as date) as effective_to,
    cast(isactive as boolean) as is_active,
    cast(isproxy as boolean) as is_proxy
from {{ ref('raw_sus_commissioner_reference_practice_commissioner') }}
where cast(relationshipstartdate as date) <= cast(relationshipenddate as date)
