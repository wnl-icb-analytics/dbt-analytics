with stg_ambulance_master_data as (

    select *
    from {{ ref('stg_ambulance_master_data') }}

)

select *
from stg_ambulance_master_data