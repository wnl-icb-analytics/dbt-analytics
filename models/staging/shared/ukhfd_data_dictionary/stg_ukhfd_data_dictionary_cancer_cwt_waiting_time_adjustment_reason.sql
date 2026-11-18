select *
from {{ ref('raw_ukhfd_data_dictionary_waiting_time_adjustment_reason') }}
where is_latest = 1