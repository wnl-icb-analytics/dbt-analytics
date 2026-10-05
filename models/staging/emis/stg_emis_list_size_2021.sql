{{
    config(
        materialized='view',
        tags=['staging', 'emis', 'reference', 'historical']
    )
}}

/*
EMIS List Size 2021 - Staging

Stages EMIS list size reference data for the 01/04/2021 historical extract.
Source: emis_list_size_2021 seed file
Extract Date: Dynamic (from seed file)

Data Quality Notes:
- Converts EXTRACT_DATE from supported EMIS export formats to DATE
- Standardizes column naming to lowercase
- Enriches borough from the NCL GP practice lookup
- Used for historical OLIDS validation
*/

with source as (
    select
        CODE,
        GP_PRACTICE,
        LIST_SIZE,
        EXTRACT_DATE
    from {{ ref('emis_list_size_2021') }}
    where EXTRACT_DATE is not null
)

select
    gp.registered_borough_name as borough,
    src.CODE as practice_code,
    src.GP_PRACTICE as practice_name,
    src.LIST_SIZE as list_size,
    coalesce(
        try_to_date(src.EXTRACT_DATE, 'DD/MM/YYYY'),
        try_to_date(src.EXTRACT_DATE, 'DD-MON-YY'),
        try_to_date(src.EXTRACT_DATE, 'DD-Mon-YY'),
        try_to_date(src.EXTRACT_DATE, 'DD-MON-YYYY'),
        try_to_date(src.EXTRACT_DATE)
    ) as extract_date
from source as src
left join {{ ref('practice_wnl_active') }} as gp
    on src.CODE = gp.practice_code
    and gp.sub_icb_code = {{ ncl_sub_icb() }}
