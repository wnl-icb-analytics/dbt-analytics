{{ 
    config(materialized='view') 
}}

select 
    date_from_parts(
        year(crtpdate),
        month(crtpdate),
        1
    ) as data_period_month,
    mode(rp_start_date) as ingestion_date,
    count(1) as row_count_estimate
from {{ ref('stg_cwt_pathway_cwt001data') }}

where crtpdate is not null

group by data_period_month

--Needed to filter out records from the next month appearing early due to hitting another checkpoint.
--Also removes very historic records without a complete month of data appearing due to hitting a later checkpoint. 
having row_count_estimate >= 1000

order by data_period_month desc