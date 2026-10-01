select
    conditionid,
    conditionname,
    productid,
    primaryterm
from {{ ref('raw_common_ccmc_dmd') }}
