-- Aggregate-only profile. No patient keys or row-level values leave Snowflake.
-- Coverage denominators are delivered ECDS attendances, not all clinical care.
-- Live comparisons require aligned source and fact refreshes. For this review, use
-- the same-snapshot reconciliation and coverage in the companion notebook.
-- Numeric quantiles across reported units are diagnostic, not standardised measures.

select 'observation_live_reconciliation' as profile,
 count(*) as joined_rows,
 count_if(s.visit_occurrence_id is null) as fact_only_rows,
 count_if(f.visit_occurrence_id is null) as source_only_rows,
 count_if(s.visit_occurrence_id is not null and f.visit_occurrence_id is not null and (not equal_null(s.observation_code, f.observation_code) or not equal_null(s.observation_value, f.observation_value) or not equal_null(s.observed_at, f.observed_at) or not equal_null(s.is_code_approved, f.is_code_approved) or not equal_null(s.source_row_id, f.source_row_id) or not equal_null(s.dmic_import_log_id, f.dmic_import_log_id) or not equal_null(s.ucum_unit_code, f.ucum_unit_code))) as altered_source_fields,
 count_if(f.visit_occurrence_id is not null and f.sk_patient_id is null) as patient_key_missing,
 count_if(f.visit_occurrence_id is not null and f.observed_at is null) as timestamp_missing,
 count_if(f.observed_at::date > current_date()) as future_timestamp_rows,
 count_if(f.observed_at::date < f.attendance_date) as before_arrival_date,
 count_if(f.observed_at::date > dateadd(day, 7, f.attendance_date)) as over_week_after_arrival,
 min(date_trunc('month',f.observed_at))::date as first_event_month,
 max(date_trunc('month',f.observed_at))::date as last_event_month
from {{ ref('stg_sus_ecds_clinical_coded_observations') }} s
full outer join {{ ref('fct_sus_uec_observation') }} f
 on s.visit_occurrence_id=f.visit_occurrence_id and s.source_sequence=f.source_sequence;

select 'assessment_live_reconciliation' as profile,
 count(*) as joined_rows,
 count_if(s.visit_occurrence_id is null) as fact_only_rows,
 count_if(f.visit_occurrence_id is null) as source_only_rows,
 count_if(s.visit_occurrence_id is not null and f.visit_occurrence_id is not null and (not equal_null(s.assessment_tool_code, f.assessment_tool_code) or not equal_null(s.person_score, f.person_score) or not equal_null(s.validated_at, f.validated_at) or not equal_null(s.is_code_approved, f.is_code_approved) or not equal_null(s.source_row_id, f.source_row_id) or not equal_null(s.dmic_import_log_id, f.dmic_import_log_id))) as altered_source_fields,
 count_if(f.visit_occurrence_id is not null and f.sk_patient_id is null) as patient_key_missing,
 count_if(f.visit_occurrence_id is not null and f.validated_at is null) as timestamp_missing,
 count_if(f.validated_at::date > current_date()) as future_timestamp_rows,
 count_if(f.validated_at::date < f.attendance_date) as before_arrival_date,
 count_if(f.validated_at::date > dateadd(day, 7, f.attendance_date)) as over_week_after_arrival,
 min(date_trunc('month',f.validated_at))::date as first_event_month,
 max(date_trunc('month',f.validated_at))::date as last_event_month
from {{ ref('stg_sus_ecds_clinical_coded_scored_assessments') }} s
full outer join {{ ref('fct_sus_uec_scored_assessment') }} f
 on s.visit_occurrence_id=f.visit_occurrence_id and s.source_sequence=f.source_sequence;

with flags as (
 select visit_occurrence_id, 1 as has_observation, 0 as has_assessment
 from {{ ref('fct_sus_uec_observation') }} group by 1
 union all
 select visit_occurrence_id, 0, 1 from {{ ref('fct_sus_uec_scored_assessment') }} group by 1
), attendance_flags as (
 select visit_occurrence_id, max(has_observation) as has_observation, max(has_assessment) as has_assessment
 from flags group by 1
)
select 'annual_coverage' as profile, year(e.start_date) as attendance_year,
 count(*) as attendances, count_if(f.has_observation=1) as with_observation,
 count_if(f.has_assessment=1) as with_assessment,
 count_if(f.has_observation=1 and f.has_assessment=1) as with_both,
 count_if(e.department_type='01') as type1_attendances,
 count_if(e.department_type='01' and f.has_observation=1) as type1_with_observation,
 count_if(e.department_type='01' and f.has_assessment=1) as type1_with_assessment
from {{ ref('obt_encounter_uec') }} e left join attendance_flags f using (visit_occurrence_id)
group by 2 order by 2;

with flags as (
 select visit_occurrence_id, 1 as has_observation, 0 as has_assessment
 from {{ ref('fct_sus_uec_observation') }} group by 1
 union all
 select visit_occurrence_id, 0, 1 from {{ ref('fct_sus_uec_scored_assessment') }} group by 1
), attendance_flags as (
 select visit_occurrence_id, max(has_observation) as has_observation, max(has_assessment) as has_assessment
 from flags group by 1
)
select 'monthly_coverage' as profile, date_trunc('month',e.start_date)::date as attendance_month,
 count(*) as attendances, count_if(f.has_observation=1) as with_observation,
 count_if(f.has_assessment=1) as with_assessment
from {{ ref('obt_encounter_uec') }} e left join attendance_flags f using (visit_occurrence_id)
where e.start_date >= '2025-01-01' and e.start_date < current_date()
group by 2 order by 2;

with flags as (
 select visit_occurrence_id, 1 as has_observation, 0 as has_assessment
 from {{ ref('fct_sus_uec_observation') }} group by 1
 union all
 select visit_occurrence_id, 0, 1 from {{ ref('fct_sus_uec_scored_assessment') }} group by 1
), attendance_flags as (
 select visit_occurrence_id, max(has_observation) as has_observation, max(has_assessment) as has_assessment
 from flags group by 1
)
select 'provider_coverage_2025' as profile, e.organisation_id, e.organisation_name,
 count(*) as attendances, count_if(f.has_observation=1) as with_observation,
 count_if(f.has_assessment=1) as with_assessment
from {{ ref('obt_encounter_uec') }} e left join attendance_flags f using (visit_occurrence_id)
where e.start_date >= '2025-01-01' and e.start_date < '2026-01-01' and e.department_type='01'
group by 2,3 having count(*)>=10000 order by attendances desc;

with flags as (
 select visit_occurrence_id, 1 as has_observation, 0 as has_assessment
 from {{ ref('fct_sus_uec_observation') }} group by 1
 union all
 select visit_occurrence_id, 0, 1 from {{ ref('fct_sus_uec_scored_assessment') }} group by 1
), attendance_flags as (
 select visit_occurrence_id, max(has_observation) as has_observation, max(has_assessment) as has_assessment
 from flags group by 1
)
select 'age_department_coverage_2025' as profile, e.department_type,
 case when e.age_at_event < 16 then 'under_16' when e.age_at_event >=16 then '16_plus' else 'unknown' end as age_group,
 count(*) as attendances, count_if(f.has_observation=1) as with_observation,
 count_if(f.has_assessment=1) as with_assessment
from {{ ref('obt_encounter_uec') }} e left join attendance_flags f using (visit_occurrence_id)
where e.start_date >= '2025-01-01' and e.start_date < '2026-01-01'
group by 2,3 having count(*)>=1000 order by 2,3;

select 'observation_terminology' as profile, code_description_source,
 count(*) as records, count(distinct observation_code) as distinct_codes,
 count_if(is_code_approved) as approved_rows, count_if(is_code_approved=false) as unapproved_rows,
 count_if(is_code_approved is null) as approval_missing,
 count_if(measurement_category is not null) as categorised,
 count_if(value_parse_status='numeric') as numeric_values,
 count_if(value_parse_status='not_recorded') as absent_values,
 count_if(value_parse_status='non_numeric_or_out_of_range') as text_or_unparseable
from {{ ref('fct_sus_uec_observation') }} group by 2 order by records desc;

select 'observation_code_profile' as profile, observation_code as code, observation_description as description,
 code_description_source, measurement_category, count(*) as records,
 count(distinct visit_occurrence_id) as attendances,
 count_if(value_parse_status='numeric') as numeric_values,
 count_if(value_parse_status='not_recorded') as absent_values,
 count_if(observation_value_numeric<0) as negative_values,
 count_if(observation_value_numeric!=trunc(observation_value_numeric)) as non_integer_values,
 round(approx_percentile(observation_value_numeric,0.01),3) as numeric_p01,
 round(approx_percentile(observation_value_numeric,0.50),3) as numeric_p50,
 round(approx_percentile(observation_value_numeric,0.99),3) as numeric_p99
from {{ ref('fct_sus_uec_observation') }}
where regexp_like(observation_code, '[0-9]{6,18}')
group by 2,3,4,5 having count(*)>=1000 order by records desc;

with signatures as (select visit_occurrence_id, observation_code, observation_value, observed_at, ucum_unit_code, count(*) as n
 from {{ ref('fct_sus_uec_observation') }} group by visit_occurrence_id, observation_code, observation_value, observed_at, ucum_unit_code)
select 'observation_repeated_signatures' as profile, sum(n) as records,
 count_if(n>1) as repeated_groups, sum(iff(n>1,n,0)) as records_in_repeated_groups,
 sum(n-1) as extra_sequences_with_same_content from signatures;

select 'assessment_terminology' as profile, code_description_source,
 count(*) as records, count(distinct assessment_tool_code) as distinct_codes,
 count_if(is_code_approved) as approved_rows, count_if(is_code_approved=false) as unapproved_rows,
 count_if(is_code_approved is null) as approval_missing,
 count_if(measurement_category is not null) as categorised,
 count_if(value_parse_status='numeric') as numeric_values,
 count_if(value_parse_status='not_recorded') as absent_values,
 count_if(value_parse_status='non_numeric_or_out_of_range') as text_or_unparseable
from {{ ref('fct_sus_uec_scored_assessment') }} group by 2 order by records desc;

select 'assessment_code_profile' as profile, assessment_tool_code as code, assessment_description as description,
 code_description_source, measurement_category, count(*) as records,
 count(distinct visit_occurrence_id) as attendances,
 count_if(value_parse_status='numeric') as numeric_values,
 count_if(value_parse_status='not_recorded') as absent_values,
 count_if(person_score_numeric<0) as negative_values,
 count_if(person_score_numeric!=trunc(person_score_numeric)) as non_integer_values,
 round(approx_percentile(person_score_numeric,0.01),3) as numeric_p01,
 round(approx_percentile(person_score_numeric,0.50),3) as numeric_p50,
 round(approx_percentile(person_score_numeric,0.99),3) as numeric_p99
from {{ ref('fct_sus_uec_scored_assessment') }}
where regexp_like(assessment_tool_code, '[0-9]{6,18}')
group by 2,3,4,5 having count(*)>=1000 order by records desc;

with signatures as (select visit_occurrence_id, assessment_tool_code, person_score, validated_at, count(*) as n
 from {{ ref('fct_sus_uec_scored_assessment') }} group by visit_occurrence_id, assessment_tool_code, person_score, validated_at)
select 'assessment_repeated_signatures' as profile, sum(n) as records,
 count_if(n>1) as repeated_groups, sum(iff(n>1,n,0)) as records_in_repeated_groups,
 sum(n-1) as extra_sequences_with_same_content from signatures;

select 'unit_terminology' as profile, unit_match_status, unit_definition_source,
 count(*) as records, count(distinct ucum_unit_code) as distinct_reported_units,
 count_if(observation_code='1104441000000107') as acvpu_rows
from {{ ref('fct_sus_uec_observation') }} group by 2,3 order by records desc;

select 'categorical_responses' as profile,
 case when categorical_value_description is not null then 'recognised_acvpu'
      when nullif(trim(observation_value),'') is null then 'absent'
      when value_parse_status='numeric' then 'numeric_in_acvpu'
      else 'unrecognised_text' end as response_status, count(*) as records
from {{ ref('fct_sus_uec_observation') }} where observation_code='1104441000000107'
group by 2 order by records desc;

select 'unit_by_observation' as profile, observation_code,
 observation_description, count(*) as records,
 count_if(unit_match_status='not_recorded') as unit_missing,
 count_if(unit_match_status='unmatched') as unit_unmatched,
 count_if(unit_match_status in ('ucum_code','dictionary_symbol','known_alias')) as unit_resolved
from {{ ref('fct_sus_uec_observation') }} where code_description_source='ecds_etos'
group by 2,3 order by records desc;
