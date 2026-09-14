select
    code,
    meaning,
    display,
    usage
from {{ ref('raw_dictionary_ers_e_referral_reason') }}
