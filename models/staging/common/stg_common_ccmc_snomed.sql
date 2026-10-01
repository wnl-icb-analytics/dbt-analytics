select
    conditionid,
    conditionname,
    conceptid,
    primaryterm
from {{ ref('raw_common_ccmc_snomed') }}
