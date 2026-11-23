select 
    am.record_id,
    am.d62__investigating_trust_code as d62__accountable_diagnostic_trust_code,
    am.d62__treating_trust_code as d62__accountable_treatment_trust_code,
    am.d62__allocation_method,
    sid.scenario_id as d62__6_scenario_id

from {{ ref('int_cwt_pathway__62d_allocation_method') }} am

--Join to get the 6 Scenario ID
left join {{ ref('int_cwt_pathway__62d_scenario_id') }} sid
on am.record_id = sid.record_id