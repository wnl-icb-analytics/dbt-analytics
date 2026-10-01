select
    sk_specialty_id,
    bk_specialty_code,
    specialty_name,
    specialty_category,
    is_treatment_function,
    is_main_specialty,
    date_created,
    date_updated,
    main_specialty_description,
    treatment_function_description
from {{ ref('raw_dictionary_dbo_specialties') }}