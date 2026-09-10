select
    code,
    meaning,
    display
from {{ ref('raw_dictionary_ers_adviceresponseoutcome') }}
