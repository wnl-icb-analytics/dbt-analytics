select
    enriched.*
from {{ ref('int_ambulance_data_enriched') }} as enriched
where {{ clean_flex_or_freeze('enriched.dlp_flex_or_freeze') }} = 'Flex'
