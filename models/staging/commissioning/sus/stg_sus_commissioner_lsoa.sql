select
    trim(oacode) as lsoa_code,
    trim(organisationcode_commissioner) as commissioner_code,
    cast(effectivefrom as date) as effective_from,
    cast(effectiveto as date) as effective_to
from {{ ref('raw_sus_commissioner_reference_lsoa_commissioner') }}
where cast(effectivefrom as date) <= cast(effectiveto as date)
