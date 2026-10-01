select
    sk_discharge_destination_id,
    bk_discharge_destination_code,
    discharge_destination_name
from {{ ref('raw_dictionary_ip_dischargedestination') }}
