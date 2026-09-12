select
    trim(term_id) as code,
    coalesce(nullif(trim(term_198), ''), nullif(trim(term_60), ''), nullif(trim(term_30), '')) as term,
    term_status,
    in_source_data = 1 as is_in_latest_source,
    effective_from as source_effective_from_at,
    import_date as source_imported_at
from {{ ref('raw_ukhfd_ctv3_terms') }}
where nullif(trim(term_id), '') is not null
-- Retired terms remain available. Identical ingestion replays can share a revision.
qualify row_number() over (
    partition by trim(term_id)
    order by effective_from desc nulls last, import_date desc nulls last,
        created_date desc nulls last, term_status, term_198, term_60, term_30
) = 1
