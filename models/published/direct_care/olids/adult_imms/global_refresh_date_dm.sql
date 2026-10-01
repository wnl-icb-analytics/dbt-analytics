{{
    config(
        materialized='view',
        tags=['adult_imms']
    )
}}
--publish global refresh date for OLIDS data models
select GLOBAL_DATA_REFRESH_DATE 
from {{ ref('int_global_data_refresh_date') }}
