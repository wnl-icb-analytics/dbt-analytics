select
    codelist_org,
    codelist_slug,
    codelist_name,
    version_hash,
    coding_system,
    code,
    term,
    cluster_id
from {{ ref('raw_reference_opencodelists') }}
where code is not null
qualify
    row_number() over (
        partition by
            codelist_org,
            codelist_slug,
            codelist_name,
            version_hash,
            coding_system,
            code,
            cluster_id
        order by
            case when term is null then 1 else 0 end,
            term
    ) = 1
