-- Synthetic SNOMED CT expressions for the IAPT procedure parser. Returns cases whose
-- interpretation differs from the expected form, focus, context or asserted procedure.
with cases as (
    select *
    from (values
        ('304891004', 'bare_concept', '304891004', null, '304891004'),
        ('304891004 |Cognitive behaviour therapy|', 'bare_concept', '304891004', null, '304891004'),
        ('304891004:408730004=410527000', 'procedure_context', '304891004', '410527000', null),
        ('304891004 : 408730004 |Procedure context| = 385658003 |Done|',
            'procedure_context', '304891004', '385658003', '304891004'),
        ('304891004:408730004=385658003,408731000=410512000',
            'unrecognised_refinement', '304891004', null, null),
        ('304891004+443730003', 'unrecognised_refinement', '304891004', null, null),
        ('x304891004', 'unparsed', null, null, null),
        ('12345678901234567890', 'unparsed', null, null, null)
    ) as t (expression, expected_type, expected_focus_code, expected_context_code, expected_asserted_code)
)

select *
from cases
where {{ iapt_procedure_expression_type('expression') }} is distinct from expected_type
    or {{ iapt_procedure_focus_code('expression') }} is distinct from expected_focus_code
    or {{ iapt_procedure_context_code('expression') }} is distinct from expected_context_code
    or {{ iapt_asserted_procedure_code('expression') }} is distinct from expected_asserted_code
