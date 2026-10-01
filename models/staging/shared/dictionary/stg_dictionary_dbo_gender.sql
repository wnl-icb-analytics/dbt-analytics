select sk_gender_id,
    gender,
    gender_code,
    gender_code1,
    date_created,
    date_updated,
    gender_code2
from {{ref('raw_dictionary_dbo_gender')}}