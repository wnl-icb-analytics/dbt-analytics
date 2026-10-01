-- Stable source examples identified during review. These guard the *00 lookup
-- key without requiring every historical/out-of-area code to resolve.
select visit_occurrence_id, 'attendance_source' as field_name
from {{ ref('int_sus_uec_encounter') }}
where attendance_source_organisation_site_identifier = 'RQM00'
  and attendance_source_organisation_name is null

union all

select visit_occurrence_id, 'receiving_organisation' as field_name
from {{ ref('int_sus_uec_encounter') }}
where receiving_site_id = 'RQM00'
  and receiving_organisation_name is null

union all

select visit_occurrence_id, 'hillingdon_site' as field_name
from {{ ref('int_sus_uec_encounter') }}
where site_id in ('AD904', 'RAS01')
  and site_name is null

union all

select visit_occurrence_id, 'independent_provider' as field_name
from {{ ref('int_sus_uec_encounter') }}
where organisation_id in ('AD9', 'NLO')
  and organisation_name is null

union all

select visit_occurrence_id, 'assigned_commissioner' as field_name
from {{ ref('int_sus_uec_encounter') }}
where assigned_commissioner_code_at_event in ('93C00', '06N00', '14Y00')
  and assigned_commissioner_name_at_event is null
