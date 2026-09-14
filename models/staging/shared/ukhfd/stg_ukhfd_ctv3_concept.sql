select
    trim(read_code) as code,
    concept_status,
    linguistic_role,
    subject_type,
    in_source_data = 1 as is_in_latest_source,
    effective_from as source_effective_from_at,
    import_date as source_imported_at
from {{ ref('raw_ukhfd_ctv3_concepts') }}
where nullif(trim(read_code), '') is not null
qualify row_number() over (
    partition by trim(read_code)
    order by effective_from desc nulls last, import_date desc nulls last,
        created_date desc nulls last, concept_status, linguistic_role, subject_type
) = 1
