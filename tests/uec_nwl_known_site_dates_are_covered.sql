-- Every dated ECDS attendance using a known NWL site/department combination
-- must fall within one of that combination's configured effective periods.
with source_attendances as (
    select
        attendance_location_site as site_code
        , attendance_location_department_type as department_type
        , attendance_arrival_date as arrival_date
    from {{ ref('stg_sus_ecds_emergency_care') }}
    where attendance_arrival_date is not null
),
nwl_site_periods as (
    select
        site_code
        , department_type
        , effective_from
        , coalesce(effective_to, '9999-12-31'::date) as effective_to
    from {{ ref('uec_site_label') }}
    where icb_footprint = 'NWL'
)
select source_attendances.*
from source_attendances
where exists (
    select 1
    from nwl_site_periods
    where nwl_site_periods.site_code = source_attendances.site_code
      and nwl_site_periods.department_type = source_attendances.department_type
)
and not exists (
    select 1
    from nwl_site_periods
    where nwl_site_periods.site_code = source_attendances.site_code
      and nwl_site_periods.department_type = source_attendances.department_type
      and source_attendances.arrival_date
        between nwl_site_periods.effective_from and nwl_site_periods.effective_to
)
