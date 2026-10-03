{% macro assign_sus_commissioner(practice_code, lsoa_code, provider_code, activity_date) %}
    /*
      Equivalent of legacy DBA.dbo.fn_AssignCommissioner.

      MIN provides deterministic handling for the small number of overlapping
      effective-date records in the handed-over reference data. The priority
      between lookup types remains identical to the SQL Server function.
    */
    coalesce(
        (
            select min(lookup.commissioner_code)
            from {{ ref('stg_sus_commissioner_practice') }} as lookup
            where lookup.practice_code = trim({{ practice_code }})
              and cast({{ activity_date }} as date)
                    between lookup.effective_from and lookup.effective_to
        ),
        (
            select min(lookup.commissioner_code)
            from {{ ref('stg_sus_commissioner_lsoa') }} as lookup
            where lookup.lsoa_code = trim({{ lsoa_code }})
              and cast({{ activity_date }} as date)
                    between lookup.effective_from and lookup.effective_to
        ),
        (
            select min(lookup.commissioner_code)
            from {{ ref('stg_sus_commissioner_provider') }} as lookup
            where lookup.provider_code = trim({{ provider_code }})
              and cast({{ activity_date }} as date)
                    between lookup.effective_from and lookup.effective_to
        ),
        (
            select min(lookup.commissioner_code)
            from {{ ref('stg_sus_commissioner_provider_postcode') }} as lookup
            where lookup.provider_code = trim({{ provider_code }})
              and cast({{ activity_date }} as date)
                    between lookup.effective_from and lookup.effective_to
        )
    )
{% endmacro %}
