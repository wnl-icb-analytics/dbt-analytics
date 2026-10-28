select 
    record_id,
    performance__metric,
    performance__date_full,
    year(performance__date_full) as performance__date_year,
    month(performance__date_full) as performance__date_month,
    performance__trust_code,
    performance__site_code,
    (
        performance__trust_code in {{ wnl_analytics.ncl_trust_codes_full() }} or
        performance__trust_code in {{ wnl_analytics.nwl_trust_codes_full() }}
    ) as performance__geo_wnl,
    performance__trust_code in {{ wnl_analytics.ncl_trust_codes_full() }} as performance__geo_ncl,
    performance__trust_code in {{ wnl_analytics.nwl_trust_codes_full() }} as performance__geo_nwl,
    performance__pathway_length_days,
    performance__numerator,
    performance__denominator,
    performance__breach
    
from {{ ref('int_cwt_pathway__performance_combined')}} cwt