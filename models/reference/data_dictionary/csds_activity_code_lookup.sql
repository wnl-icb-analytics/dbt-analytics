with definitions as (
    select
        code_set_name
        , source_code_set_name
        , code
        , description
        , short_description
        , category
        , notes
        , valid_from_date
        , valid_to_date
        , is_currently_valid
        , source_imported_at
        , source_effective_from_at as definition_updated_at
        , 'UKHFD NHS Data Dictionary' as definition_source
    from {{ ref('csds_activity_code_lookup_history') }}
    where is_latest_definition

    union all

    -- UKHFD has no infant physical examination result list; ETOS publishes the permitted values.
    select
        'infant_physical_examination_result'
        , 'CSDS ETOS ' || source_worksheet
        , code
        , description
        , null::varchar
        , null::varchar
        , null::varchar
        , null::date
        , null::date
        , null::boolean
        , null::timestamp_ntz
        , null::timestamp_ntz
        , 'CSDS ETOS v' || specification_version
    from {{ ref('csds_infant_physical_examination_result') }}
)

select *
from definitions
qualify row_number() over (
    partition by code_set_name, code
    order by
        definition_source = 'UKHFD NHS Data Dictionary' desc
        , source_code_set_name = 'Community_Care_Activity_Type' desc
        , source_code_set_name = 'Referral_Closure_Reason' desc
        , definition_updated_at desc nulls last
        , source_imported_at desc nulls last
        , source_code_set_name
) = 1
