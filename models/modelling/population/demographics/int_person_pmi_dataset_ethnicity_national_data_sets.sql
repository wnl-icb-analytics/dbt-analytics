select
    'eth' as dataset_source,
    sk_patientid as sk_patient_id,
    --Trim to fix records with trailing spaces
    left(
        case 
            when trim(ethnicity_code) = '' then null
            when right(ethnicity_code, 1) = '*' then left(ethnicity_code, length(ethnicity_code) - 1)
            else trim(ethnicity_code) 
        end, 
        1
    ) as ethnicity_code,
    cast(record_date as date) as ethnicity_event_date
    
from {{ ref('stg_reference_lookup_ncl_ethnicity_national_data_sets') }}
