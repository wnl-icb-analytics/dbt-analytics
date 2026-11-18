{{ config(materialized="table", tags=["tat", "daily"]) }}

-- Supported analyst table for imaging Turnaround Times. Query this model, not
-- DATA_LAKE__NCL.ANALYST_MANAGED.TURNAROUND_TIMES_RAW (retired copy). Keep
-- DATA_LAKE.TAT.TURNAROUND_TIMES_RAW as the ingest landing table.
-- Applies the original R/Python pipeline rules on staged rows:
--   * TAT hours (scan / report / overall)
--   * Flex/Freeze classification (datedifftest window), out-of-range rows dropped
--   * cancer pathway flag standardised to Y/N/Unclassified
--   * restatement: only the latest submission per trust + data period survives

with staged as (

    select * from {{ ref('stg_tat_turnaround_times') }}

),

calculated as (

    select
        *,
        coalesce(priority_type_code, 1)                                                                       as priority_type_code_routine_default,
        round(datediff('second', diagnostic_test_request_date_time, diagnostic_test_date_time)    / 3600.0)   as tat_scan,
        round(datediff('second', diagnostic_test_date_time,         service_report_issue_date_time) / 3600.0) as tat_report,
        round(datediff('second', diagnostic_test_request_date_time, service_report_issue_date_time) / 3600.0) as tat_overall,
        datediff('month', data_period, date_trunc('month', diagnostic_test_date_time))                        as datedifftest,
        case
            when upper(cancer_pathway_flag) in ('TRUE', '1', 'Y')  then 'Y'
            when upper(cancer_pathway_flag) in ('FALSE', '0', 'N') then 'N'
            else 'Unclassified'
        end                                                                                                   as cancer_pathway_flag_string
    from staged
    -- mandatory date fields (mirrors the original pipeline's row filter)
    where diagnostic_test_date_time is not null
      and diagnostic_test_request_date_time is not null

),

classified as (

    select
        *,
        case
            -- one historical file is always Freeze (carried over from functions.R)
            when file_name = '20241028_DIDNCL_RAN_Jul24.csv' or datedifftest = -3 then 'Freeze'
            when datedifftest in (-2, -1)                                          then 'Flex'
            else 'Out of range'
        end as data_type
    from calculated

),

final as (

    select
        tat_event_id,
        submission_date,
        data_type,
        month,
        year,
        trust_code,
        data_period,
        ethnic_category,
        person_gender,
        general_medical_practice,
        patient_source_type,
        referrer_code,
        referring_organisation,
        diagnostic_test_request_date_time,
        diagnostic_test_request_received_date_time,
        diagnostic_test_date_time,
        service_report_issue_date_time,
        imaging_code_nicip,
        imaging_code_snomed,
        combined_imaging_code,
        provider_site_code,
        priority_type_code,
        priority_type_code_routine_default,
        cancer_pathway_flag,
        cancer_pathway_flag_string,
        tat_scan,
        tat_report,
        tat_overall,
        datedifftest,
        file_name,
        source_file,
        loaded_at
    from classified
    where data_type <> 'Out of range'
    -- restatement: keep only the latest submission FILE per trust + nominal period.
    -- dense_rank (not max(submission_date)) so that if two files share a submission_date,
    -- the loaded_at / file_name tiebreakers pick exactly one file; all its rows share the
    -- same sort key and survive together. (Freeze periods stay put; later Flex files win.)
    qualify dense_rank() over (
        partition by trust_code, data_period
        order by submission_date desc, loaded_at desc, file_name desc
    ) = 1

)

select * from final
