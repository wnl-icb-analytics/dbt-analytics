-- A small tail of genuinely unknown site codes exists in the source. This
-- threshold protects the established coverage while allowing that known tail.
with coded_sites_without_names as (
    select count(*) as unmatched_attendance_count
    from {{ ref('stg_sus_ecds_emergency_care') }} as attendance
    left join {{ ref('organisation_nhs_site') }} as site
        on attendance.attendance_location_site = site.organisation_code
    where attendance.attendance_arrival_date >= '2024-04-01'::date
      and attendance.attendance_location_site is not null
      and site.organisation_code is null
)

select *
from coded_sites_without_names
where unmatched_attendance_count > 1000
