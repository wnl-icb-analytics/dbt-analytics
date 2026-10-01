select
    sk_source_of_referral,
    bk_source_of_referral_code,
    referral_type,
    referral_group,
    date_created,
    date_updated
from {{ ref('raw_dictionary_op_sourceofreferrals') }}