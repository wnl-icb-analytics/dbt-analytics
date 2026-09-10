-- A seven-character Read v2 code identifies the exact five-character code and two-character term.
select
    trim(read_code) || trim(term_code) as code
    , trim(read_code) as read_code
    , trim(term_code) as term_code
    , coalesce(nullif(trim(term_198), ''), nullif(trim(term_60), ''), nullif(trim(term_30), '')) as term
    , status_flag
    , language_code
    , in_source_data = 1 as is_in_latest_source
    , effective_from as source_effective_from_at
    , effective_to as source_effective_to_at
    , import_date as source_imported_at
    , created_date as source_created_at
from {{ ref('raw_ukhfd_read_v2_terms') }}
where nullif(trim(read_code), '') is not null and nullif(trim(term_code), '') is not null
-- Repeated ingestion rows can share a term revision. Keep all codes, including retired terms.
qualify row_number() over (
    partition by trim(read_code), trim(term_code)
    order by effective_from desc nulls last, import_date desc nulls last,
        created_date desc nulls last, unique_column desc
) = 1
