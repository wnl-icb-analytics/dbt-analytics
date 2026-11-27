with ambulance_data as (

    select *
    from {{ ref('stg_ambulance_master_data') }}

)

select
    ambulance_data.*,
    call_hrg.hrg_name as call_hrg_description,
    incident_hrg.hrg_name as incident_hrg_description
from ambulance_data
left join {{ ref('hrg_code') }} as call_hrg
    on upper(trim(ambulance_data.call_hrg_code)) = upper(trim(call_hrg.hrg_code))
left join {{ ref('hrg_code') }} as incident_hrg
    on upper(trim(ambulance_data.incident_hrg_code)) = upper(trim(incident_hrg.hrg_code))
