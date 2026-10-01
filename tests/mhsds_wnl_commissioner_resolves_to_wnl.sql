select source_record_id
from {{ ref('fct_mhsds_referral') }}
where commissioner_organisation_code = 'Z9B2Z'
    and source_derived_icb_commissioner_code is null
    and derived_icb_commissioner_code is distinct from 'Z9B2Z'

union all

select source_record_id
from {{ ref('fct_mhsds_care_contact') }}
where commissioner_organisation_code = 'Z9B2Z'
    and source_derived_icb_commissioner_code is null
    and derived_icb_commissioner_code is distinct from 'Z9B2Z'
