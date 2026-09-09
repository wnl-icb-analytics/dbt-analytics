-- Retired organisation and site codes must remain available for historical activity.
with source_codes as (
    select organisation_code
    from {{ ref('stg_ukhfd_ods_api_organisation') }}
    where organisation_code is not null
    union
    select upper(trim(organisation_code))
    from {{ ref('stg_dictionary_dbo_organisation') }}
    where nullif(trim(organisation_code), '') is not null
    union
    select organisation_code
    from {{ ref('stg_ukhfd_ods_closed_organisation') }}
)
select source_codes.organisation_code
from source_codes
left join {{ ref('organisation') }} as names
    on source_codes.organisation_code = names.organisation_code
where names.organisation_code is null
