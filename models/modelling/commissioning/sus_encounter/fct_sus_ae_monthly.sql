{{
    config(materialized = 'table')
}}

--------------------------------------------------------------------------------
-- Migrated from [ETL].[postProcess_AE_Current].
--
-- The stored procedure ran a series of UPDATE statements against the loaded
-- fact table. Here each UPDATE becomes a column expression instead: the row is
-- built correct once rather than rewritten seven times.
--
-- The SP's step order turned out NOT to be load-bearing -- no step consumed a
-- column an earlier step had written -- so this is a single SELECT rather than
-- a chain of CTEs. If a future step does depend on an earlier one, it has to
-- become its own CTE layer.
--
-- SP steps implemented here:
--   1. z_arrival_datetime + 6 z_em_*_datetime columns  (macro ae_event_datetime)
--   2. z_commissioning_access = 2 for duplicate attendances
--   3. z_pod_level_4                                   (macro update_pod_ae)
--   4. z_provider_site_code
--   6. z_ccg_code                                      (macro assign_sus_commissioner)
--
-- SP steps NOT implemented (blocked, tagged [TODO] inline):
--   5. z_imd_2015_*                  -- source not yet in Snowflake (parked)
--   7. z_care_home                   -- source not yet in Snowflake (parked)
--
-- A SECOND stored procedure, dbo.processBusinessRules, is also migrated here
-- as the trailing CTE layers (rules_applied -> contract_typed -> final). It
-- has to run after the derivations above because several of its rules test
-- z_provider_site_code and z_financial_year. See macro ae_business_rules for
-- the 30 rules themselves and the parity notes on each.
--
-- SP steps intentionally dropped: the blocks commented out in the original
-- (sensitive data category, NHS number pseudo, reason for access, zCCGCode via
-- udf_zCCGCodeFromFields, LSOA01). These were dead code in the SP -- the
-- "Sandpit does not contain..." notes explain why. They stay NULL, as they
-- were in the legacy table.
--------------------------------------------------------------------------------

{#-
    Rules are held in seqno order in the macro for traceability against the
    legacy table, but sorted alphabetically here: the stored procedure built
    zBusinessRule with ORDER BY br_code, so the pipe-delimited string must be
    in code order to match.
-#}
{% set rules = ae_business_rules() | sort(attribute='code') %}

with int_ae as (

    select * from {{ ref('int_sus_ae_monthly') }}

),

keyed as (

    -- SP step 2, part 1. The SP built this key into #temp1, numbered it in
    -- #temp2, then joined back on (SourceItemID, RowID). None of that is
    -- needed here: the numbering happens in the same pass as the rows it
    -- labels, so no row key and no temp tables.
    select
        *
        ,coalesce(sk_patient_id::varchar, local_patient_identifier, '0')
            || ' | ' || coalesce(organisation_code_code_of_provider, '0')
            || ' | ' || coalesce(to_char(to_date(arrival_date), 'DD-MM-YY'), '0')
            || ' | ' || coalesce(to_char(to_time(arrival_time), 'HH24:MI:SS'), '0')
            as dupe_id
    from int_ae

),

flagged as (

    -- SP step 2, part 2. Split from the key derivation above so the
    -- partition key is a column reference rather than the full concatenation
    -- repeated inline.
    select
        *
        ,case
            -- 20220908: sk_patient_id = 1 is excluded from dedup entirely.
            -- Those rows keep whatever z_commissioning_access they arrived with.
            when sk_patient_id = 1 then null
            -- [DETERMINISM] The SP ordered by DupeID, i.e. by the partition
            -- key itself, so which duplicate survived was arbitrary and could
            -- differ between runs. Ordered by sk_encounter_id here so the same
            -- row wins every time.
            else row_number() over (
                partition by dupe_id
                order by sk_encounter_id
            )
         end as dupe_row_number
    from keyed

),

derived as (

    select
    -- [STRUCTURE] Snowflake threw internal error 300002 when these expressions
    -- sat inside SELECT * REPLACE (...). Dropping REPLACE and excluding the
    -- overwritten columns instead avoids it. Consequence: the 12 derived
    -- columns below now appear at the END of the column list rather than in
    -- their original positions. Immaterial for anything selecting by name;
    -- matters only for positional / select * comparisons against legacy.
    f.* exclude (
        dupe_id
        ,dupe_row_number
        ,z_arrival_datetime
        ,z_em_assessment_waiting_datetime
        ,z_em_attendance_conclusion_datetime
        ,z_em_datetime_seen_for_treatment
        ,z_em_departure_datetime
        ,z_em_duration_datetime
        ,z_em_initial_assessment_datetime
        ,z_em_treatment_wait_datetime
        ,z_commissioning_access
        ,z_provider_site_code
        ,z_pod_level_4
        ,z_ccg_code
        ,z_care_home
    )

        ------------------------------------------------------------------
        -- SP step 1: bare event times -> full timestamps
        ------------------------------------------------------------------
        ,timestamp_ntz_from_parts(
            to_date(arrival_date),
            to_time(arrival_time)
        ) as z_arrival_datetime

        -- NOTE: em_assessment_waiting_time is currently a NULL stub in the int
        -- model, so this resolves to NULL. Expression is live and will populate
        -- itself if that column is ever sourced.
        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_assessment_waiting_time') }}
            as z_em_assessment_waiting_datetime

        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_attendance_conclusion_time') }}
            as z_em_attendance_conclusion_datetime

        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_time_seen_for_treatment') }}
            as z_em_datetime_seen_for_treatment

        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_departure_time') }}
            as z_em_departure_datetime

        -- em_duration_time is a DURATION IN MINUTES, not a clock
        -- time -- values cluster hard at 239/240, i.e. the four-hour A&E
        -- target. The SP converted it to a 1900-anchored datetime and used
        -- T-SQL's datetime + datetime, which adds the DAY count. So 240
        -- minutes became 240 days: arrival 2021-04-01 with a 87-minute
        -- duration produced 2021-06-27 in the legacy table. This has been corrected for this model.
       ,dateadd(minute, em_duration_time,
            timestamp_ntz_from_parts(to_date(arrival_date), to_time(arrival_time)))
                as z_em_duration_datetime

        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_initial_assessment_time') }}
            as z_em_initial_assessment_datetime

        -- NOTE: em_treatment_wait_time is also a NULL stub in the int model.
        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_treatment_wait_time') }}
            as z_em_treatment_wait_datetime

        -- z_em_conclusion_waiting_datetime is deliberately absent: the SP block
        -- for it was commented out (no EM Conclusion Waiting Time in Sandpit).

        ------------------------------------------------------------------
        -- SP step 2: flag duplicate attendances
        --   2 = duplicate (second and subsequent rows for the same
        --       patient / provider / arrival timestamp)
        --   1 = in-area commissioner, first occurrence
        --   0 = out-of-area
        -- Per the 20210924 note, only 2s flow through to consolidated.
        ------------------------------------------------------------------
        ,case
            when dupe_row_number > 1 then 2
            else z_commissioning_access
         end as z_commissioning_access

        ------------------------------------------------------------------
        -- SP step 4: resolve provider site code
        -- organisation_code_code_of_provider is ALREADY truncated to 3 chars
        -- in int_sus_ae_monthly, so the LEFT(...,3) wrapper the SP used on
        -- every branch is dropped here.
        -- [CASE SENSITIVITY] SQL Server's default collation is case
        -- insensitive, so 'pfh' matched 'PFH'. Snowflake's = is case
        -- sensitive, hence the upper() wrappers on the affected branches.
        ------------------------------------------------------------------
        ,case
            when organisation_code_code_of_provider = 'RYJ'
                then substr(unique_cds_identifier, 2, 5)
            when organisation_code_code_of_provider = 'RQN' then 'RYJ03'
            when organisation_code_code_of_provider = 'RJ5' then 'RYJ01'
            when organisation_code_code_of_provider = 'RQM'
                then substr(provider_reference_no, 1, 5)
            when organisation_code_code_of_provider = 'RAS'
                and em_department_type in ('01', '1') then 'RAS01'
            when organisation_code_code_of_provider = 'RAS'
                and em_department_type in ('03', '3') then 'RAS02'
            when provider_site_code = 'NTP'
                and em_department_type in ('03', '3')
                then left(organisation_code_code_of_provider, 5)
            when organisation_code_code_of_provider = 'RV8'
                and provider_site_code = 'RV846'
                and em_department_type in ('03', '3') then 'RC303'
            when organisation_code_code_of_provider = 'RV8'
                and provider_site_code = 'RV847'
                and em_department_type in ('03', '3') then 'NTP60'
            when organisation_code_code_of_provider = 'RC3'
                and em_department_type in ('01', '1') then 'RC368'
            when organisation_code_code_of_provider = 'RY9'
                and em_department_type in ('03', '3') then 'RY901'
            when organisation_code_code_of_provider = 'RFW'
                and em_department_type in ('01', '1') then 'RFW00'
            when organisation_code_code_of_provider in ('RV8', 'RAS', 'RFW')
                then left(provider_site_code, 5)
            when provider_site_code in ('RYX', 'RYX01') then 'RYX01'
            when provider_site_code = 'RYX02' then 'RYX02'
            when provider_site_code = 'RYX03' then 'RYX03'
            when provider_site_code = 'RYX23' then 'RYX23'
            when provider_site_code = 'RYX11' then 'RYX11'
            when upper(nhs_service_agreement_line_no) = 'PFH'
                and upper(provider_reference_no) = 'PFH' then 'PFH00'
            when upper(left(em_attendance_number, 2)) = 'NP'
                and organisation_code_code_of_provider = 'R1K' then 'R1K01'
            when upper(left(em_attendance_number, 2)) = 'ED'
                and organisation_code_code_of_provider = 'R1K' then 'R1K04'
            else left(provider_site_code, 5)
         end as z_provider_site_code

        ------------------------------------------------------------------
        -- SP step 3: point of delivery band
        ------------------------------------------------------------------
        ,{{ update_pod_ae('arrival_date', 'core_hrg', 'em_patient_group', 'em_department_type') }}
            as z_pod_level_4

        ------------------------------------------------------------------
        -- SP step 6: assign commissioner
        -- Argument order follows the UDF call in the SP:
        --   gp practice, lsoa, provider, activity date
        -- The SP wrapped the first three in ISNULL(...,''). Those coalesces
        -- are kept here; drop them if the macro handles nulls itself.
        -- Arrival date is passed bare: the SP's ISNULL([Arrival Date],'')
        -- relied on T-SQL coercing a date to an empty string, which will not
        -- work in Snowflake.
        ------------------------------------------------------------------
        ,{{ assign_sus_commissioner(
              "coalesce(gp_practice_code, '')",
              "coalesce(z_lsoa11, '')",
              "coalesce(organisation_code_code_of_provider, '')",
              "arrival_date"
           ) }} as z_ccg_code

        ------------------------------------------------------------------
        -- SP steps 5 and 7 go here once the sources land. Both need a dedup
        -- guard before joining -- IMD should be 1:1 on LSOA but is not
        -- guaranteed to be, and care home spells can overlap in time, which
        -- would multiply rows. Left as NULL for now (already NULL in int).
        --
        --   ,imd.z_imd_2015_london_quintile as z_imd_2015_london_quintile   -- [TODO]
        --   ,imd.z_imd_2015_national_decile as z_imd_2015_national_decile   -- [TODO]
        ,ch.care_home_code as z_care_home
    from flagged f
    left join {{ ref('int_sus_ae_care_home') }} as ch
        on f.sk_encounter_id = ch.sk_encounter_id

),

--------------------------------------------------------------------------------
-- Migrated from [dbo].[processBusinessRules].
--
-- The stored procedure looped a cursor over dbo.br_rules_updates, executing
-- each rule's stored SQL text as dynamic SQL, writing matches to a temp table,
-- pivoting that into a per-rule column table, then string-concatenating the
-- matched rule names back into zBusinessRule. All of that machinery exists
-- only because the rules were data; here they are predicates, so one pass
-- evaluates every rule at once.
--------------------------------------------------------------------------------

rules_applied as (

    select
        *

        -- One flag per rule. array_construct_compact drops the NULLs, leaving
        -- only the codes that matched.
        ,array_construct_compact(
{%- for rule in rules %}
            iff({{ rule.condition }}, '{{ rule.code }}', null){{ "," if not loop.last }}
{%- endfor %}
         ) as matched_rule_codes

        -- Same shape, but carrying each rule's contract type. Held in the same
        -- alphabetical order as the codes above, and deliberately NOT
        -- deduplicated: the legacy procedure replaced names with type codes
        -- one-for-one, so two rules of the same type produce '|1|1'.
        ,array_construct_compact(
{%- for rule in rules %}
            iff({{ rule.condition }}, '{{ rule.contract_type }}', null){{ "," if not loop.last }}
{%- endfor %}
         ) as matched_contract_types

    from derived

),

contract_typed as (

    select
        -- z_business_rule and z_contract_type arrive from the sandpit view as
        -- NULL stubs; they are dropped here and rebuilt below.
        * exclude (
            matched_rule_codes
            ,matched_contract_types
            ,z_business_rule
            ,z_contract_type
        )

        -- Pipe-delimited list of matched rule names, alphabetically ordered --
        -- the legacy FOR XML PATH concatenation used ORDER BY br_code. NULL
        -- when no rule matched, as in legacy.
        ,case
            when array_size(matched_rule_codes) = 0 then null
            else '|' || array_to_string(matched_rule_codes, '|')
         end as z_business_rule

        -- Contract type string. 99 is the sentinel for "no rule matched" and
        -- is resolved in the next layer.
        ,case
            when array_size(matched_contract_types) = 0 then '99'
            else '|' || array_to_string(matched_contract_types, '|')
         end as z_contract_type_raw

    from rules_applied

),

sla_matched as (

    -- SLA lookup, used only to resolve the 99 sentinel. distinct guards against
    -- fan-out; the seed has a uniqueness test but the join must not depend on
    -- it holding.
    select distinct ccg, provider, z_financial_year
    from {{ ref('ref_br_rules_sla') }}

)

select
    d.* exclude (z_contract_type_raw)

    ------------------------------------------------------------------
    -- Contract type resolution, in the legacy procedure's own order:
    --   1. rows containing 99 (no rule matched, or the UCC_WMX rule whose
    --      contract type is itself 99) become 1 where an SLA exists for the
    --      commissioner / provider / financial year, else 2
    --   2. anything still containing 6 collapses to just 6
    --   3. anything containing 7 collapses to 2
    --
    -- NOTE: no active rule has contract type 7, so step 3 is currently dead.
    -- Retained for parity in case a rule is reactivated.
    --
    -- NOTE: the SLA seed only covers financial years up to 1718, so unmatched
    -- attendances from 1819 onwards always resolve to 2.
    ------------------------------------------------------------------
    ,case
        when replace(
                d.z_contract_type_raw,
                '99',
                iff(s.ccg is not null, '1', '2')
             ) like '%6%' then '|6'
        when replace(
                d.z_contract_type_raw,
                '99',
                iff(s.ccg is not null, '1', '2')
             ) like '%7%' then '|2'
        else replace(
                d.z_contract_type_raw,
                '99',
                iff(s.ccg is not null, '1', '2')
             )
     end as z_contract_type

from contract_typed d
left join sla_matched s
    on left(d.organisation_code_code_of_commissioner,3) = s.ccg
    and d.organisation_code_code_of_provider = s.provider
    and d.z_financial_year = s.z_financial_year
-- [TODO] SP step 5 -- once index_of_multiple_deprivation_2015 is a source:
-- left join (
--     select z_lsoa11, z_imd_2015_london_quintile, z_imd_2015_national_decile
--     from {{ '{{' }} source('dmic_reference', 'index_of_multiple_deprivation_2015') {{ '}}' }}
--     qualify row_number() over (partition by z_lsoa11 order by z_lsoa11) = 1
-- ) imd on flagged.z_lsoa11 = imd.z_lsoa11