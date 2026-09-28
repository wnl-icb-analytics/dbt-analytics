select code::varchar as code, display as name
from {{ ref('stg_dictionary_ers_gender') }}
