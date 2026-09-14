-- Synthetic examples check exact-match precedence and the historical rule's limits.
with examples as (
    select column1::varchar as example_name, column2::varchar as snomed_code,
        column3::varchar as supplied_unit, column4::varchar as expected_symbol
    from values
        ('weight_case', '27113001', 'Kg', 'kg'),
        ('weight_word', '27113001', 'KILOGRAMS', 'kg'),
        ('height_centimetres', '50373000', 'CM', 'cm'),
        ('height_metres', '50373000', 'M', 'm'),
        ('bmi_case', '60621009', 'KG/M2', 'kg/m2'),
        ('bmi_superscript', '60621009', 'KG/M²', 'kg/m2'),
        ('wrong_measurement', '60621009', 'Kg', null),
        ('unknown_measurement', null, 'Kg', null),
        ('numeric_placeholder', '27113001', '0', null),
        ('exact_unit_any_context', null, 'kg', 'kg')
)
select e.example_name
from examples as e
left join {{ ref('clinical_unit_of_measurement') }} as exact_unit
    on trim(e.supplied_unit) = exact_unit.code
left join {{ ref('csds_observation_unit_alias') }} as alias_unit
    on exact_unit.code is null
    and e.snomed_code = alias_unit.snomed_code
    and upper(trim(e.supplied_unit)) = alias_unit.source_unit_upper
where coalesce(exact_unit.unit_symbol, alias_unit.unit_symbol) is distinct from e.expected_symbol
