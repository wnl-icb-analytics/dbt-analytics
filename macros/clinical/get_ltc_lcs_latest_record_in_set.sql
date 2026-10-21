{% macro get_ltc_lcs_latest_record_in_set(inclusion_valuesets, test_valuesets) %}

{# Rank the full inclusion pool before testing the kept observation's code membership.
   Membership uses observation IDs so mapped and source-code matches both count.

   inclusion_valuesets: value sets forming each person's pool; the latest record is kept.
   test_valuesets: value sets the kept record must belong to.
   Returns person_id, observation_id, clinical_effective_date and result_value, one row per
   person whose kept record passes the test; no row otherwise. Undated records rank last;
   same-date records are resolved by observation_id descending (EMIS gives no tie rule).
   Empty value set arguments raise a compiler error in get_ltc_lcs_observations. #}

select latest.person_id, latest.observation_id, latest.clinical_effective_date, latest.result_value
from (
    select distinct person_id, observation_id, clinical_effective_date, result_value
    from ({{ get_ltc_lcs_observations(inclusion_valuesets) }})
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc nulls last, observation_id desc
    ) = 1
) as latest
where exists (
    select 1
    from ({{ get_ltc_lcs_observations(test_valuesets) }}) as code_test
    where code_test.observation_id = latest.observation_id
)

{% endmacro %}
