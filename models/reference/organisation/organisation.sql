with ods as (
    select
        organisation_code
        , organisation_name
        , 'UKHFD ODS API' as name_source
        , source_effective_from_at
    from {{ ref('stg_ukhfd_ods_api_organisation') }}
    where organisation_code is not null
    qualify row_number() over (
        partition by organisation_code
        order by source_effective_from_at desc nulls last
            , source_last_changed_date desc nulls last
            , source_register_id desc nulls last
            , organisation_name
    ) = 1
), dictionary as (
    select
        nullif(upper(trim(organisation_code)), '') as organisation_code
        , nullif(trim(organisation_name), '') as organisation_name
        , 'Dictionary' as name_source
        , coalesce(last_updated, first_created)::timestamp_ntz as source_effective_from_at
    from {{ ref('stg_dictionary_dbo_organisation') }}
    where nullif(trim(organisation_code), '') is not null
    qualify row_number() over (
        partition by upper(trim(organisation_code))
        order by coalesce(last_updated, first_created) desc nulls last
            , sk_organisation_id desc nulls last
            , organisation_name
    ) = 1
)
select organisation_code, organisation_name, name_source, source_effective_from_at
from ods
union all
-- Dictionary retains historical and local codes absent from the ODS register.
select organisation_code, organisation_name, name_source, source_effective_from_at
from dictionary
where not exists (
    select 1 from ods where ods.organisation_code = dictionary.organisation_code
)
union all
-- The closed archive retains older codes absent from both maintained registers.
select organisation_code, organisation_name,
    'UKHFD ODS closed archive' as name_source, source_effective_from_at
from {{ ref('stg_ukhfd_ods_closed_organisation') }} as archive
where not exists (
    select 1 from ods where ods.organisation_code = archive.organisation_code
)
and not exists (
    select 1 from dictionary where dictionary.organisation_code = archive.organisation_code
)
