select
    trim(oa_code) as lsoa_code,
    trim(organisation_code_commissioner) as commissioner_code,
    cast(effective_from as date) as effective_from,
    cast(effective_to as date) as effective_to
from {{ ref('raw_sus_commissioner_reference_lsoa_commissioner') }}
where cast(effective_from as date) <= cast(effective_to as date)
