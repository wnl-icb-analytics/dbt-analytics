select
    cluster_id,
    min(cluster_description) as cluster_description,
    code,
    min(code_description) as code_description,
    source
from {{ ref('raw_reference_combined_codesets') }}
where code is not null
group by cluster_id, code, source
