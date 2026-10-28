select *
from {{ ref('raw_ukhfd_data_dictionary_two_week_wait_cancer_or_symptomatic_breast_referral_type') }}
where is_latest = 1