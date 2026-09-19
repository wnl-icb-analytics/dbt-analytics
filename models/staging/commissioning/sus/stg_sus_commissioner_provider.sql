select
    trim(providercode) as provider_code,
    trim(commissionercode) as commissioner_code,
    cast(effectivefrom as date) as effective_from,
    cast(effectiveto as date) as effective_to
from {{ ref('raw_sus_commissioner_reference_provider_commissioner') }}
where cast(effectivefrom as date) <= cast(effectiveto as date)
