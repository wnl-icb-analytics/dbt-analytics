-- Historical ETOS aliases apply only to their named measurement concept.
with aliases as (
    select column1::varchar as snomed_code, column2::varchar as source_unit_upper,
        column3::varchar as canonical_unit_code, column4::varchar as source_data_item
    from values
        ('27113001', 'KG', 'kg', 'C202D48'),
        ('27113001', 'KILOGRAMS', 'kg', 'C202D48'),
        ('50373000', 'CM', 'cm', 'C202D47'),
        ('50373000', 'M', 'm', 'C202D47'),
        ('60621009', 'KG/M2', 'kg/m2', 'C201D49'),
        ('60621009', 'KG/M²', 'kg/m2', 'C201D49')
)
select a.snomed_code, a.source_unit_upper, a.canonical_unit_code,
    u.unit_symbol, u.description, u.quantity_name,
    'CSDS ETOS historical unit alias' as definition_source,
    u.definition_source as unit_definition_source,
    a.source_data_item,
    '2022-09-08'::date as source_change_date
from aliases as a
left join {{ ref('clinical_unit_of_measurement') }} as u on a.canonical_unit_code = u.code
