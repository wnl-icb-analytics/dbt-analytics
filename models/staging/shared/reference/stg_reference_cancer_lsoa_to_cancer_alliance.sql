
select
    lsoa21cd as lsoa_2021_code,
    lsoa21nm as lsoa_2021_name,
    sicbl25cd as ons_sub_icb_code,
    sicbl25cdh as sub_icb_code,
    sicbl25nm as sub_icb_name,
    icb25cd as ons_icb_code,
    icb25cdh as icb_code,
    icb25nm as icb_name,
    cal25cd as cancer_alliance_code,
    cal25nm as cancer_alliance_name,
    lad25cd as borough_code,
    lad25nm as borough_name

from {{ ref('lsoa_to_cancer_alliance') }}