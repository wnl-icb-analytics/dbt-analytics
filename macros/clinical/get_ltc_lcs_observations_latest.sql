{% macro get_ltc_lcs_observations_latest(valuesets, match_paths='both') %}

{# Latest observation per person and value set, using the shared matching rules.
   P2 root permissions also retain local descriptions of permitted public concepts.
   Same-day records use EMIS save time, then observation ID. #}

select * exclude (date_recorded)
from ({{ get_ltc_lcs_observations(valuesets, match_paths=match_paths, include_date_recorded=true, exclude_result_headers=true) }})
qualify row_number() over (
    partition by person_id, valueset_id
    order by clinical_effective_date desc nulls last, date_recorded desc nulls last, observation_id desc
) = 1

{% endmacro %}
