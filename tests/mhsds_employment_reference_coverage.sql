-- These published statuses must not disappear when a partial speciality list is used.
with required_codes as (
    select column1::varchar as code
    from values ('01'), ('02'), ('03'), ('04'), ('05'), ('06'), ('07'), ('08'), ('97'), ('98'), ('99'), ('UU'), ('ZZ')
)
select e.code
from required_codes as e
left join {{ ref('mhsds_employment_status') }} as actual
    on e.code = actual.code
where actual.description is null
