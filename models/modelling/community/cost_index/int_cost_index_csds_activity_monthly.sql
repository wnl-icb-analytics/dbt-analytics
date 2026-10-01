/*
CSDS proxy cost per patient and calendar month.

Grain: sk_patient_id x activity_month. Activity counts every contact row;
proxy_cost is zero for non-costed contacts.
*/

select
    sk_patient_id
    , date_trunc('month', care_contact_date)::date             as activity_month
    , 'Community Health Contact'                               as service
    , sum(proxy_cost)                                          as total_cost
    , count(*)                                                 as total_activity
    , 'contact'                                                as activity_unit
from {{ ref('fct_csds_currency_contacts') }}
where sk_patient_id is not null
group by
    sk_patient_id
    , date_trunc('month', care_contact_date)::date
