select
    deficit,
    detail_key,
    weight
from {{ ref('raw_common_efi2_weights') }}
