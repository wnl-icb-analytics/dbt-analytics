-- Each site code and department type must have non-overlapping effective
-- periods, otherwise the arrival-date join in int_sus_uec_encounter fans out.
select
    a.site_code
    , a.department_type
    , a.effective_from as period_a_from
    , a.effective_to as period_a_to
    , b.effective_from as period_b_from
    , b.effective_to as period_b_to
from {{ ref('organisation_uec_site') }} as a
join {{ ref('organisation_uec_site') }} as b
    on a.site_code = b.site_code
    and a.department_type = b.department_type
    and a.effective_from < b.effective_from
    and a.effective_to >= b.effective_from
