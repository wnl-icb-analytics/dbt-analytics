with latest_history as (
    select code_set_name, code, source_code_set_name
    from {{ ref('csds_activity_code_lookup_history') }}
    where is_latest_definition
)

, missing_codes as (
    select distinct
        history.code_set_name
        , history.code
        , 'missing_historical_code' as failure_reason
    from latest_history as history
    left join {{ ref('csds_activity_code_lookup') }} as code_lookup
        on history.code_set_name = code_lookup.code_set_name
        and history.code = code_lookup.code
    where code_lookup.code is null
)

, incorrect_precedence as (
    select
        code_lookup.code_set_name
        , code_lookup.code
        , 'current_dictionary_not_preferred' as failure_reason
    from {{ ref('csds_activity_code_lookup') }} as code_lookup
    inner join latest_history as expected
        on code_lookup.code_set_name = expected.code_set_name
        and code_lookup.code = expected.code
    where (
        expected.code_set_name = 'activity_type'
        and expected.source_code_set_name = 'Community_Care_Activity_Type'
        or expected.code_set_name = 'referral_closure_reason'
        and expected.source_code_set_name = 'Referral_Closure_Reason'
    )
        and code_lookup.source_code_set_name != expected.source_code_set_name
)

select code_set_name, code, failure_reason from missing_codes
union all
select code_set_name, code, failure_reason from incorrect_precedence
