{{ 
    config(materialized='view') 
}}

select

    cwt.record_id,

    --NCL
    coalesce(cwt.la_of_residence in {{ ncl_borough_codes() }}, false) as ncl_geo_flag__residence_at_event,
    coalesce(subicb_reg.sub_icb_code in {{ ncl_sub_icb_incl_ccg() }}, false) as ncl_geo_flag__registered_at_event,
    coalesce(
        org__consultant_upgrade_trust_code in {{ ncl_trust_codes_full() }} or
        org__first_seen_trust_code in {{ ncl_trust_codes_full() }} or
        org__faster_diagnosis_pathway_end_trust_code in {{ ncl_trust_codes_full() }} or
        org__patient_pathway_identifier_issuer_trust_code in {{ ncl_trust_codes_full() }} or
        org__accountable_investigating_trust_code in {{ ncl_trust_codes_full() }} or
        org__accountable_treating_trust_code in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving1 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving2 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving3 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving4 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving5 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving6 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving7 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving8 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving9 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving10 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreferring1 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreferring2 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreferring3 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreferring4 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreferring5 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreferring6 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreferring7 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreferring8 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreferring9 in {{ ncl_trust_codes_full() }} or
        cwt.organisationidentifierreferring10 in {{ ncl_trust_codes_full() }},

        false
        
    ) as ncl_geo_flag__trust,

    ncl_geo_flag__residence_at_event or ncl_geo_flag__registered_at_event or ncl_geo_flag__trust as ncl_geo_flag__any,

    --NWL
    coalesce(cwt.la_of_residence in {{ nwl_borough_codes() }}, false) as nwl_geo_flag__residence_at_event,
    coalesce(subicb_reg.sub_icb_code in {{ nwl_sub_icb_incl_ccg() }}, false) as nwl_geo_flag__registered_at_event,
    coalesce(
        org__consultant_upgrade_trust_code in {{ nwl_trust_codes_full() }} or
        org__first_seen_trust_code in {{ nwl_trust_codes_full() }} or
        org__faster_diagnosis_pathway_end_trust_code in {{ nwl_trust_codes_full() }} or
        org__patient_pathway_identifier_issuer_trust_code in {{ nwl_trust_codes_full() }} or
        org__accountable_investigating_trust_code in {{ nwl_trust_codes_full() }} or
        org__accountable_treating_trust_code in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving1 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving2 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving3 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving4 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving5 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving6 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving7 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving8 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving9 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreceiving10 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreferring1 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreferring2 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreferring3 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreferring4 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreferring5 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreferring6 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreferring7 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreferring8 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreferring9 in {{ nwl_trust_codes_full() }} or
        cwt.organisationidentifierreferring10 in {{ nwl_trust_codes_full() }},

        false
        
    ) as nwl_geo_flag__trust,

    nwl_geo_flag__residence_at_event or nwl_geo_flag__registered_at_event or nwl_geo_flag__trust as nwl_geo_flag__any

    /* In the original script for debugging but unused so commented out here
    --Pathway checkpoint specific flags
    org__consultant_upgrade_trust_code in {{ ncl_trust_codes_full() }} as ncl_geo_flag__consultant_upgrade_trust,
    org__first_seen_trust_code in {{ ncl_trust_codes_full() }} as ncl_geo_flag__first_seen_trust,
    org__faster_diagnosis_pathway_end_trust_code in {{ ncl_trust_codes_full() }} as ncl_geo_flag__faster_diagnosis_pathway_end_trust,
    org__patient_pathway_identifier_issuer_trust_code in {{ ncl_trust_codes_full() }} as ncl_geo_flag__patient_pathway_identifier_issuer_trust,
    org__accountable_investigating_trust_code in {{ ncl_trust_codes_full() }} as ncl_geo_flag__accountable_investigating_trust,
    org__accountable_treating_trust_code in {{ ncl_trust_codes_full() }} as ncl_geo_flag__accountable_treating_trust
    */

from {{ ref('int_cwt_pathway__data_tidy') }} cwt

--Join to determine NCL Registration

--Join to get borough of practce
left join {{ ref('practice_national_all') }} subicb_reg
on cwt.pds_gpcode = subicb_reg.practice_code
