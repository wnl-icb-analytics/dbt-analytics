select
    r.*
    , st.serv_team_type_ref_to_mh as currency_service_or_team_type_code
    , label.description as currency_service_or_team_type_description
    , rr.population_category as currency_referral_reason_category
    , tt.population_category as currency_team_type_category
    , tt.setting_group as currency_setting_group
    , tt.setting_name as currency_setting_name
    , {{ mhsds_is_crisis_referral('tt', 'r.clinical_response_priority_code') }}
        as is_currency_crisis_referral
from {{ ref('fct_mhsds_referral_summary') }} as r
left join {{ ref('int_mhsds_currency_referral_service_type') }} as st
    on r.uniq_serv_req_id = st.uniq_serv_req_id
left join {{ ref('mhsds_service_or_team_type') }} as label
    on st.serv_team_type_ref_to_mh = label.code
left join {{ ref('nhse_mh_currency_referral_reasons_2627') }} as rr
    on r.primary_reason_for_referral_code = rr.prim_reason_referral_mh
left join {{ ref('nhse_mh_currency_team_types_2627') }} as tt
    on st.serv_team_type_ref_to_mh = tt.serv_team_type
