-- Every published version of the UKHSA COVID and flu code cluster workbooks, one row
-- per programme, spec version, cluster and code. source mirrors the COMBINED_CODESETS
-- naming (UKHSA_COVID, UKHSA_FLU) so get_observations(..., versioned=true) can filter
-- on the same source value and add spec_version for the campaign to pin against.
select
    'UKHSA_' || programme as source,
    programme,
    spec_version,
    release_date,
    cluster_id,
    min(cluster_description) as cluster_description,
    snomed_code as code,
    min(snomed_description) as code_description
from {{ ref('raw_reference_ukhsa_codecluster_versions') }}
where snomed_code is not null
group by programme, spec_version, release_date, cluster_id, snomed_code
