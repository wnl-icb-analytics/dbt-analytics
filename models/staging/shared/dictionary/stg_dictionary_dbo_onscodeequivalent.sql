select geography_code, geography_name, entity_code, status,
    date_of_introduction, date_of_termination, created_date, import_date
from {{ ref('raw_dictionary_dbo_onscodeequivalent') }}
