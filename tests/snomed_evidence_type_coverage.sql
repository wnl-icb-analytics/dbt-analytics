{{ config(severity='warn') }}

-- A warning means new or unsupported code-list members await review.
select snomed_code, concept_name, classification_basis
from {{ ref('snomed_evidence_type') }}
where evidence_type is null
