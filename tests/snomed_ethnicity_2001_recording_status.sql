-- Reference codes only. Protect the distinction lost in the original seed.
with expected as (
    select column1 as snomed_code, column2 as expected_code
    from values
        ('1024701000000100', 'Z'), -- Ethnicity not stated
        ('763726001', 'Z'),       -- Patient refused to give ethnic group
        ('415794004', '99'),      -- Ethnicity unknown
        ('33897005', 'R'),        -- Chinese
        ('718958002', 'C')        -- Roma
)
select e.snomed_code, e.expected_code, m.ethnicity_2001_code
from expected e
left join {{ ref('snomed_ethnicity_2001_bridge') }} m using (snomed_code)
where m.ethnicity_2001_code is distinct from e.expected_code
