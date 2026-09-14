-- An alias label requires its documented measurement context and cannot replace an exact match.
select a.source_record_id
from {{ ref('fct_csds_care_activity') }} as a
left join {{ ref('clinical_unit_of_measurement') }} as exact_unit
    on trim(a.observation_unit_code) = exact_unit.code
left join {{ ref('read_code') }} as r
    on trim(a.observation_code) = r.code
    and ((trim(a.observation_scheme_code) = '01' and r.coding_system = 'read_v2')
        or (trim(a.observation_scheme_code) = '02' and r.coding_system = 'ctv3'))
left join {{ ref('csds_observation_unit_alias') }} as alias_unit
    on upper(trim(a.observation_unit_code)) = alias_unit.source_unit_upper
    and case when trim(a.observation_scheme_code) = '03' then trim(a.observation_code)
        else r.snomed_ct_code end = alias_unit.snomed_code
where a.observation_unit_match_type = 'csds_etos_alias'
    and (exact_unit.code is not null or alias_unit.snomed_code is null
        or a.observation_unit_symbol is distinct from alias_unit.unit_symbol
        or a.observation_unit_definition_source is distinct from alias_unit.definition_source)
