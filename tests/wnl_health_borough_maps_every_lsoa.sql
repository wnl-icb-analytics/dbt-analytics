select lsoa.lsoa_2021_code
from {{ ref('stg_reference_geo_lsoa_21_wnl') }} as lsoa
left join {{ ref('wnl_health_borough_lsoa_2021') }} as health_borough
    on lsoa.lsoa_2021_code = health_borough.lsoa_2021_code
where health_borough.lsoa_2021_code is null
