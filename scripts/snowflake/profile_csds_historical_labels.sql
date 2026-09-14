-- Read-only historical CSDS label diagnostics. Returns aggregate counts only.
-- Uses existing shared DEV models and every available UKHFD reference revision.
-- Do not interpret cross-system or term-identifier matches as confirmed clinical labels.
-- Validated on 9 September 2026; counts change as sources are refreshed.

-- Full histories: no Is_Latest, In_Source_Data, Active or status filters.
with gaps as (
 select clinical_record_type,clinical_code_system,trim(clinical_code) as code,count(*) as n
 from DEV__REPORTING.COMMUNITY.FCT_CSDS_CLINICAL_RECORD
 where clinical_description is null and clinical_code is not null
 and clinical_code_system in ('Read v2','CTV3','SNOMED CT') group by 1,2,3
), cv2 as (
 select distinct "Read_Code"::varchar as code from UKHFD."Read_Codes"."dim_CV2_SCD"
 where coalesce(nullif(trim("Pref_Term_198"),''),nullif(trim("Pref_Term_60"),''),nullif(trim("Pref_Term_30"),'')) is not null
), kv2 as (
 select distinct "Read_Code"::varchar as code from UKHFD."Read_Codes"."dim_KV2_SCD"
 where coalesce(nullif(trim("Term_198"),''),nullif(trim("Term_60"),''),nullif(trim("Term_30"),'')) is not null
), kv2_complete as (
 select distinct "Read_Code"::varchar||"Term_Code"::varchar as code from UKHFD."Read_Codes"."dim_KV2_SCD"
 where coalesce(nullif(trim("Term_198"),''),nullif(trim("Term_60"),''),nullif(trim("Term_30"),'')) is not null
), ctv3 as (
 select distinct d."Read_Code"::varchar as code
 from UKHFD."Read_Codes"."dim_CV3_Description_SCD" d
 join UKHFD."Read_Codes"."dim_CV3_Terms_SCD" t on d."Term_ID"=t."Term_ID"
 where coalesce(nullif(trim(t."Term_198"),''),nullif(trim(t."Term_60"),''),nullif(trim(t."Term_30"),'')) is not null
), sct as (
 select distinct "Concept_ID"::varchar as code from UKHFD."SNOMED"."dim_Descriptions_SCD" where nullif(trim("Term"),'') is not null
), sct_description as (
 select distinct "ID"::varchar as code from UKHFD."SNOMED"."dim_Descriptions_SCD" where nullif(trim("Term"),'') is not null
), dictionary_unqualified as (
 select distinct read_code::varchar as code from DEV__STAGING.DICTIONARY.STG_DICTIONARY_DBO_READCODES where nullif(trim(term),'') is not null
 union select distinct read_code_alt::varchar from DEV__STAGING.DICTIONARY.STG_DICTIONARY_DBO_READCODES where nullif(trim(term),'') is not null
)
select g.clinical_record_type,g.clinical_code_system,sum(g.n) as unlabelled_records,count(*) as distinct_codes,
 sum(iff(v2.code is not null,g.n,0)) as cv2_history_label_matches,
 sum(iff(kv2.code is not null,g.n,0)) as kv2_history_label_matches,
 sum(iff(kvc.code is not null,g.n,0)) as complete_read_term_matches,
 sum(iff(v3.code is not null,g.n,0)) as ctv3_history_label_matches,
 sum(iff(s.code is not null,g.n,0)) as snomed_concept_history_label_matches,
 sum(iff(sd.code is not null,g.n,0)) as snomed_description_id_matches,
 sum(iff(d.code is not null,g.n,0)) as dictionary_without_scheme_guard_matches
from gaps g left join cv2 v2 on g.code=v2.code left join kv2 on g.code=kv2.code
left join kv2_complete kvc on g.code=kvc.code left join ctv3 v3 on g.code=v3.code
left join sct s on g.code=s.code left join sct_description sd on g.code=sd.code
left join dictionary_unqualified d on g.code=d.code
group by 1,2 order by 1,2;
select 'CV2' as reference,count(*) as revisions,count(distinct "Read_Code") as codes,
 min(length("Read_Code")) as min_key_length,max(length("Read_Code")) as max_key_length
from UKHFD."Read_Codes"."dim_CV2_SCD"
union all select 'KV2',count(*),count(distinct "Read_Code"),min(length("Read_Code")),max(length("Read_Code")) from UKHFD."Read_Codes"."dim_KV2_SCD"
union all select 'CTV3',count(*),count(distinct "Read_Code"),min(length("Read_Code")),max(length("Read_Code")) from UKHFD."Read_Codes"."dim_CV3_Concept_SCD";


-- Term identifiers are distinct from concept identifiers.

with gaps as (
 select clinical_code_system,trim(clinical_code) as code,count(*) as n
 from DEV__REPORTING.COMMUNITY.FCT_CSDS_CLINICAL_RECORD
 where clinical_description is null and clinical_code is not null and clinical_code_system in ('Read v2','CTV3') group by 1,2
), terms as (
 select "Term_ID" as code,count(distinct coalesce(nullif(trim("Term_198"),''),nullif(trim("Term_60"),''),nullif(trim("Term_30"),''))) as labels
 from UKHFD."Read_Codes"."dim_CV3_Terms_SCD" group by 1
), term_keys as (select distinct "Term_Key" as code from UKHFD."Read_Codes"."dim_KV2_SCD")
select g.clinical_code_system,count(*) as codes,sum(g.n) as records,
 sum(iff(t.code is not null,g.n,0)) as ctv3_term_id_matches,
 sum(iff(t.labels=1,g.n,0)) as ctv3_unique_term_id_matches,
 sum(iff(k.code is not null,g.n,0)) as read_v2_term_key_matches
from gaps g left join terms t on g.code=t.code left join term_keys k on g.code=k.code group by 1;


-- A term identifier can link to several concepts across reference history.

with gaps as (
 select clinical_record_type,trim(clinical_code) as code,count(*) as n
 from DEV__REPORTING.COMMUNITY.FCT_CSDS_CLINICAL_RECORD
 where clinical_description is null and clinical_code is not null and clinical_code_system='CTV3' group by 1,2
), terms as (
 select "Term_ID" as code,count(distinct coalesce(nullif(trim("Term_198"),''),nullif(trim("Term_60"),''),nullif(trim("Term_30"),''))) as labels
 from UKHFD."Read_Codes"."dim_CV3_Terms_SCD" group by 1
), concepts as (
 select "Term_ID" as code,count(distinct "Read_Code") as concepts from UKHFD."Read_Codes"."dim_CV3_Description_SCD" group by 1
)
select g.clinical_record_type,count(*) as candidate_codes,sum(g.n) as candidate_records,
 sum(iff(c.concepts=1,g.n,0)) as single_concept_records,
 sum(iff(c.concepts>1,g.n,0)) as multiple_concept_records,
 sum(iff(c.code is null,g.n,0)) as no_concept_link_records
from gaps g join terms t on g.code=t.code left join concepts c on g.code=c.code group by 1;
with terms as (
 select "Term_ID" as code,count(distinct coalesce(nullif(trim("Term_198"),''),nullif(trim("Term_60"),''),nullif(trim("Term_30"),''))) as labels
 from UKHFD."Read_Codes"."dim_CV3_Terms_SCD" group by 1
), concepts as (select distinct "Read_Code" as code from UKHFD."Read_Codes"."dim_CV3_Concept_SCD")
select count(*) as ctv3_term_ids,count_if(c.code is not null) as term_ids_also_concept_codes
from terms t left join concepts c on t.code=c.code;


-- Dictionary rows without coding-system membership may contain code-only placeholders.


with gaps as (
    select clinical_code_system, trim(clinical_code) as code, count(*) as n
    from DEV__REPORTING.COMMUNITY.FCT_CSDS_CLINICAL_RECORD
    where clinical_description is null and clinical_code is not null
        and clinical_code_system in ('Read v2', 'CTV3')
    group by 1, 2
)
select g.clinical_code_system, count(*) as codes, sum(g.n) as records,
    sum(iff(r.term = 'Read Code: ' || g.code, g.n, 0)) as code_only_placeholder_records
from gaps g
join DEV__STAGING.DICTIONARY.STG_DICTIONARY_DBO_READCODES r on g.code = r.read_code
where r.is_read_v2 is null and r.is_ctv3 is null
group by 1;

-- Separate labels already resolved by the historical term lookup from remaining gaps.
select clinical_code_system, clinical_record_type, count(*) as labelled_records,
    count_if(dictionary_snomed_code is not null) as inherited_snomed_mappings
from DEV__REPORTING.COMMUNITY.FCT_CSDS_CLINICAL_RECORD
where clinical_label_source = 'UKHFD Read v2 term history'
group by 1, 2;
