-- UKHFD should label every recognised department type and almost every
-- submitted practice code. Codes 06 and 99 are known, rare source values that
-- are not valid current department types.
with coverage as (
    select
        'department_type' as field_name
        , count_if(
            department_type is not null
            and department_type_desc is null
            and department_type not in ('06', '99')
          ) as missing_labels
        , count_if(department_type is not null) as coded_attendances
    from {{ ref('int_sus_uec_encounter') }}

    union all

    select
        'registered_practice'
        , count_if(reg_practice_at_event is not null and reg_practice_name_latest is null)
        , count_if(reg_practice_at_event is not null)
    from {{ ref('int_sus_uec_encounter') }}
)

select field_name, missing_labels, coded_attendances
from coverage
where missing_labels > 0
  and missing_labels / nullif(coded_attendances, 0) > 0.001
