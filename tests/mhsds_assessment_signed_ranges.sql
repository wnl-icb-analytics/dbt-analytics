-- Synthetic examples distinguish the separator from signed bounds.
with examples as (
    select column1 as published_value, column2 as expected_minimum,
        column3 as expected_maximum
    from values
        ('0-40', 0, 40), ('-2-2', -2, 2), ('-5--1', -5, -1),
        ('-5 - -1', -5, -1), ('-2.5--0.5', -2.5, -0.5),
        ('0.0-5.0', 0, 5), ('NA', null, null), ('1', null, null)
)
select
    published_value, expected_minimum, expected_maximum,
    {{ mhsds_assessment_range_bound('published_value', 'minimum') }} as actual_minimum,
    {{ mhsds_assessment_range_bound('published_value', 'maximum') }} as actual_maximum
from examples
where actual_minimum is distinct from expected_minimum
    or actual_maximum is distinct from expected_maximum
