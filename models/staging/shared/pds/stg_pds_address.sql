{{
    config(materialized = 'table')
}}

--CTE to get all invalid row ids
with invalid_residence_rows as (
    select
        row_id
    from {{ref('raw_pds_pds_address')}}

    qualify not(
        coalesce(usual_address_business_effective_to_date, '9999-12-31') = 
        max(coalesce(usual_address_business_effective_to_date, '9999-12-31'))
        over (
            partition by pseudo_nhs_number, 
            usual_address_business_effective_from_date
        )
    )
)

select
    row_id,
    {{ consistent_sk_patient_id_format('pseudo_nhs_number')}} as sk_patient_id, 
    der_postcode_sector as postcode_sector, 
    der_currentyr2021_lsoa_of_residence_from_postcode as lsoa_21, 
    to_date(usual_address_business_effective_from_date) as event_from_date, 
    to_date(usual_address_business_effective_to_date) as event_to_date,
    der_cqc_care_home_code

from {{ref('raw_pds_pds_address')}}
--removes about 90 rows
where row_id not in (select row_id from invalid_residence_rows)
--Qualify clause to remove duplicate records and only keep a single row
qualify row_id = max(row_id) over (partition by sk_patient_id, event_to_date)