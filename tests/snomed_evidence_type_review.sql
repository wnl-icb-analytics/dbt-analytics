select review.snomed_code, review.evidence_type as expected_type,
    actual.evidence_type, review.basis
from {{ ref('snomed_evidence_type_review') }} as review
left join {{ ref('snomed_evidence_type') }} as actual using (snomed_code)
where actual.evidence_type is distinct from review.evidence_type
