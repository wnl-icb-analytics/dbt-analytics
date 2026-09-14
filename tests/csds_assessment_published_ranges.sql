with expected as (
    select column1 as concept_code, column2 as minimum_value,
        column3 as maximum_value, column4 as decimal_places
    from values
        ('952611000000105', 0, 60, 1),
        ('1053941000000101', 0, 465, 0),
        ('718426000', 14, 70, 0)
)
select e.concept_code, r.minimum_numeric_value, r.maximum_numeric_value, r.decimal_places
from expected as e
left join {{ ref('csds_assessment_scale') }} as r on e.concept_code = r.concept_code
where e.minimum_value is distinct from r.minimum_numeric_value
    or e.maximum_value is distinct from r.maximum_numeric_value
    or e.decimal_places is distinct from r.decimal_places
