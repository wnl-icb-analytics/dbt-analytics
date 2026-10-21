{% macro get_ltc_lcs_observations_latest(valuesets, match_paths='both', emis_local_exact=false) %}

{# Latest observation per person and value set, using the shared matching rules.
   emis_local_exact restricts local list roots without rejecting local descriptions
   of listed public concepts. Date and observation ID order are unchanged. #}

select *
from ({{ get_ltc_lcs_observations(valuesets, match_paths=match_paths, emis_local_exact=emis_local_exact) }})
qualify row_number() over (
    partition by person_id, valueset_id
    order by clinical_effective_date desc, observation_id desc
) = 1

{% endmacro %}
