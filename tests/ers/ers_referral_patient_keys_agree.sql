select ubrn_id
from {{ ref('fct_ers_referral_action') }}
group by ubrn_id
having count(distinct sk_patient_id) > 1
