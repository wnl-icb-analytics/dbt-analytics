-- UEC site labels (UTC, walk-in centre, MIU) with effective dates, from the
-- uec_site_label seed plus site name from the organisation dictionary.
-- One row per site code, department type and effective period. Join on
-- attendance site, department type and arrival date between the dates.

with organisation as (
    select organisation_code, organisation_name
    from {{ ref('stg_dictionary_dbo_organisation') }}
    qualify row_number() over (
        partition by organisation_code order by coalesce(last_updated, first_created) desc
    ) = 1
)

select
    s.site_code
    , s.department_type
    , s.uec_site_label
    , o.organisation_name as site_name
    , s.provider_code
    , s.icb_footprint
    , s.effective_from
    , coalesce(s.effective_to, '9999-12-31'::date) as effective_to
    , s.effective_to is null as is_current
    , s.note
from {{ ref('uec_site_label') }} as s
left join organisation as o
    on s.site_code = o.organisation_code
