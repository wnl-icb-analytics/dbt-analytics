select
    trim(provider_code) as provider_code,
    trim(commissioner_code) as commissioner_code,
    cast(effective_from as date) as effective_from,
    cast(effective_to as date) as effective_to
from {{ ref('raw_sus_commissioner_reference_provider_postcode_commissioner') }}
where cast(effective_from as date) <= cast(effective_to as date)
