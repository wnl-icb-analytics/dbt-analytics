
select 
    ccg21cd as ons_sub_icb_code,
    ccg21cdh as sub_icb_code,
    ccg21nm as sub_icb_name,
    stp21cd as ons_icb_code,
    stp21cdh as icb_code,
    stp21nm as icb_name,
    cal21cd as cancer_alliance_code,
    cal21nm as cancer_alliance_name

from {{ ref('ccg_icb_to_cancer_alliance') }}