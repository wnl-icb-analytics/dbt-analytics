select
    pld_d62.record_id,
    pld_d62.performance__date_full,
    
    --Organisation information
    pld_d62.d62__investigating_trust_code,
    pld_d62.d62__treating_trust_code,
    pld_d62.d62__treating_site_code,

    --Performance metrics
    pld_d62.performance__pathway_length_days,
    pld_d62.performance__numerator_base,
    pld_d62.performance__denominator_base,
    pld_d38.performance__numerator as breach_d38,
    pld_d24.performance__numerator as breach_d24,
    
    --Flag fields
    pld_d62.performance__numerator_base = 1 as f_62,
    breach_d38 = 1 as f_38,
    breach_d24 = 1 as f_24,

    --Scenario ID
    case
        when (    f_62  and     f_38  and     f_24 ) then 1
        when (    f_62  and     f_38  and not(f_24)) then 2
        when (    f_62  and not(f_38) and     f_24 ) then 3
        when (not(f_62) and     f_38  and not(f_24)) then 4
        when (not(f_62) and not(f_38) and     f_24 ) then 5
        when (not(f_62) and not(f_38) and not(f_24)) then 6
        else -1
    end as scenario_id,

    --Accountability Factor for Investigation
    case scenario_id
        when 1 then 0.5 -- 62 Day compliant, Investigation compliant and Treatment compliant
        when 2 then 0.5 -- 62 Day compliant, Investigation compliant and Treatment breach
        when 3 then 0   -- 62 Day compliant, Investigation breach    and Treatment compliant
        when 4 then 0   -- 62 Day breach,    Investigation compliant and Treatment compliant
        when 5 then 1   -- 62 Day breach,    Investigation breach    and Treatment compliant
        when 6 then 0.5 -- 62 Day breach,    Investigation breach    and Treatment breach
        else 'X'
    end as performance_investigating_accountability_factor,

    --Accountability Factor for Treatment
    case scenario_id
        when 1 then 0.5 -- 62 Day compliant, Investigation compliant and Treatment compliant
        when 2 then 0.5 -- 62 Day compliant, Investigation compliant and Treatment breach
        when 3 then 1   -- 62 Day compliant, Investigation breach    and Treatment compliant
        when 4 then 1   -- 62 Day breach,    Investigation compliant and Treatment compliant
        when 5 then 0   -- 62 Day breach,    Investigation breach    and Treatment compliant
        when 6 then 0.5 -- 62 Day breach,    Investigation breach    and Treatment breach
        else 'X'
    end as performance_treating_accountability_factor
    
from {{ref('int_cwt_pathway__62d_allocation_method')}} pld_d62

--Join to get 38 Day and 24 Day performance
left join {{ref('int_cwt_pathway__performance_38d')}} pld_d38
on pld_d62.record_id = pld_d38.record_id

left join {{ref('int_cwt_pathway__performance_24d')}} pld_d24
on pld_d62.record_id = pld_d24.record_id

where d62__allocation_method = '6 Scenarios'