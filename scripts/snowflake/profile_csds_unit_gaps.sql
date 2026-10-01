-- Read-only CSDS unit diagnostics. Returns aggregates and public reference metadata only.
-- Uses existing DEV facts and the shared CSDS_SIMPLE feed. No objects are changed.
-- Validated on 9 September 2026; counts change as data arrive.
-- Case-fold and Dictionary-key matches below are candidates, not authorised unit mappings.

with a as (
 select observation_unit_code as unit_code,provider_organisation_code,year(reporting_period_end_date) as reporting_year,count(*) as n
 from DEV__REPORTING.COMMUNITY.fct_csds_care_activity where observation_unit_code is not null and observation_unit_name is null group by 1,2,3
), numeric_dictionary as (
 select sk_unit_id::varchar as code,max(unit_symbol) as symbol from DEV__STAGING.DICTIONARY.stg_dictionary_dbo_unit group by 1
), casefold as (
 select upper(code) as code,count(distinct unit_symbol) as symbols,max(unit_symbol) as symbol
 from DEV__REFERENCE.TERMINOLOGY.clinical_unit_of_measurement group by 1
)
select iff(regexp_like(a.unit_code,'[0-9]+'),'numeric_only','other') as unit_shape,
 sum(a.n) as unmatched_rows,count(distinct a.unit_code) as distinct_unmatched_units,
 count(distinct provider_organisation_code) as providers,min(reporting_year) as first_year,max(reporting_year) as last_year,
 sum(iff(d.code is not null,a.n,0)) as coincidental_dictionary_key_match,
 sum(iff(c.symbols=1,a.n,0)) as unambiguous_casefold_candidate,
 sum(iff(c.symbols>1,a.n,0)) as ambiguous_casefold_candidate
from a left join numeric_dictionary d on a.unit_code=d.code
left join casefold c on upper(a.unit_code)=c.code group by 1;

-- Distinguish numeric placeholders from UCUM unity; do not decode surrogate IDs.
select count(*) as numeric_unit_rows,count_if(observation_unit_code='1') as literal_ucum_unity,
 count_if(observation_unit_code='0') as zero_token,count_if(length(observation_unit_code)>4) as longer_than_four_characters,
 count_if(observation_unit_code=observation_value::varchar) as same_as_observation_value,
 count_if(observation_code is null) as missing_observation_code
from DEV__REPORTING.COMMUNITY.fct_csds_care_activity
where observation_unit_name is null and regexp_like(observation_unit_code,'[0-9]+');
with a as (
 select contact_id,activity_id,observation_code,observation_unit_code,count(*) as n
 from DEV__REPORTING.COMMUNITY.fct_csds_care_activity
 where observation_unit_name is null and regexp_like(observation_unit_code,'[0-9]+') group by 1,2,3,4
), s as (
 select "Unique_care_contact_identifier" as contact_id,"Unique_care_activity_identifier" as activity_id,"Code" as observation_code,
 count(distinct "UCUM_unit_of_measurement") as different_units,max("UCUM_unit_of_measurement") as unit_code
 from DATA_LAKE.CSDS_SIMPLE."tblCare_Activity_Coding" where "UCUM_unit_of_measurement" is not null group by 1,2,3
)
select sum(a.n) as numeric_source_occurrences,
 sum(iff(s.activity_id is not null,a.n,0)) as present_in_simple,
 sum(iff(s.different_units=1 and s.unit_code=a.observation_unit_code,a.n,0)) as same_unit_in_simple,
 sum(iff(s.different_units=1 and s.unit_code<>a.observation_unit_code,a.n,0)) as changed_unit_in_simple,
 sum(iff(s.different_units>1,a.n,0)) as ambiguous_simple_units
from a left join s on a.contact_id=s.contact_id and a.activity_id=s.activity_id and a.observation_code=s.observation_code;

-- Hypothetical Dictionary-key results demonstrate why coincident integers are insufficient.
with a as (
 select observation_unit_code as unit_code,
 case when trim(observation_scheme_code) in ('03', '3') and trim(observation_code)='27113001' then 'weight' when trim(observation_scheme_code) in ('03', '3') and trim(observation_code)='50373000' then 'height' when trim(observation_scheme_code) in ('03', '3') and trim(observation_code)='60621009' then 'bmi' else 'other_observation' end as observation_category,
 count(*) as n from DEV__REPORTING.COMMUNITY.fct_csds_care_activity
 where observation_unit_name is null and regexp_like(observation_unit_code,'[0-9]+') group by 1,2
)
select a.observation_category,d.unit_symbol as hypothetical_dictionary_symbol,d.quantity_name as hypothetical_quantity,sum(a.n) as affected_rows
from a left join DEV__STAGING.DICTIONARY.stg_dictionary_dbo_unit d on a.unit_code=d.sk_unit_id::varchar
group by 1,2,3 having sum(a.n)>=1000 order by 4 desc;
