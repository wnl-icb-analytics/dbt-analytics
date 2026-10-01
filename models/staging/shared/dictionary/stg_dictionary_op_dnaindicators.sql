select
    sk_dna_indicator_id,
    bk_dna_code,
    dna_indicator_desc,
    dna_indicator_status,
    date_created,
    date_updated
from {{ ref('raw_dictionary_op_dnaindicators') }}