{% macro get_ltc_lcs_latest_record_in_set(inclusion_valuesets, test_valuesets, emis_local_exact=false) %}

{# Rank the full inclusion pool before testing the kept observation's code membership.
   Membership uses observation IDs so mapped and source-code matches both count.

   inclusion_valuesets: value sets forming each person's pool; the latest record is kept.
   test_valuesets: value sets the kept record must belong to.
   emis_local_exact: restrict local list roots using the shared observation matcher.
   Returns person_id, observation_id, clinical_effective_date and result_value, one row per
   person whose kept record passes the test; no row otherwise. Undated records rank last.
   Same-date records rank by date_recorded (EMIS save time) descending, then lowest result,
   then observation_id descending for records saved together without a result.
   Empty value set arguments raise a compiler error in get_ltc_lcs_observations. #}

select latest.person_id, latest.observation_id, latest.clinical_effective_date, latest.result_value
from (
    select distinct person_id, observation_id, clinical_effective_date, date_recorded, result_value
    from ({{ get_ltc_lcs_observations(inclusion_valuesets, emis_local_exact=emis_local_exact, include_date_recorded=true) }})
    qualify row_number() over (
        partition by person_id
        order by
            clinical_effective_date desc nulls last,
            date_recorded desc nulls last,
            result_value asc nulls last,
            observation_id desc
    ) = 1
) as latest
where exists (
    select 1
    from ({{ get_ltc_lcs_observations(test_valuesets, emis_local_exact=emis_local_exact) }}) as code_test
    where code_test.observation_id = latest.observation_id
)

{% endmacro %}
