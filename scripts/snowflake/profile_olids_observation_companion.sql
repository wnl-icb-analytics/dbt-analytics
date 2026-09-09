-- Aggregate-only pre-publication impact checks. No source identifiers are returned.
-- Native observation history is not rescanned for Valproate; the existing output is the baseline.

-- Pre-publication aggregate impact only. Uses existing materialised Valproate events.
with codes as (
 select distinct code from STAGING.REFERENCE.STG_REFERENCE_VALPROATE_PROG_CODES where code_category='REFERRAL'
), added as (
 select 'allergy_intolerance' as source_entity,a.id,a.patient_id,a.clinical_effective_date,a.mapped_concept_code,a.mapped_concept_display
 from OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE a join codes c on a.mapped_concept_code=c.code
 where coalesce(a.lds_is_deleted,false)=false and a.person_id is not null
 union all
 select 'referral_request',a.id,a.patient_id,a.clinical_effective_date,a.mapped_concept_code,a.mapped_concept_display
 from OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST a join codes c on a.mapped_concept_code=c.code
 where coalesce(a.lds_is_deleted,false)=false and a.person_id is not null
), candidate as (
 select a.*,p.person_id from added a join MODELLING.OLIDS_PERSON_ATTRIBUTES.INT_PATIENT_PERSON_UNIQUE p on a.patient_id=p.patient_id
), old_events as (
 select * from MODELLING.OLIDS_PROGRAMME.INT_VALPROATE_ARAF_REFERRAL_EVENTS
)
select 'existing_events' as population,count(*) as records,count(distinct person_id) as people,
 count(distinct araf_referral_id) as distinct_event_ids,null::number as matching_existing_event_rows,
 null::number as changed_date_or_concept_rows from old_events
union all
select c.source_entity,count(*),count(distinct c.person_id),count(distinct c.id),count_if(o.araf_referral_id is not null),
 count_if(o.araf_referral_id is not null and (c.clinical_effective_date is distinct from o.araf_referral_event_date or c.mapped_concept_code is distinct from o.araf_referral_concept_code or c.mapped_concept_display is distinct from o.araf_referral_concept_display))
from candidate c left join old_events o on c.person_id=o.person_id and c.id=o.araf_referral_id group by 1;

-- Only native dates after the existing consensus date need to be checked.
with baseline as (
 select global_data_refresh_date as refresh_date from MODELLING.OLIDS_UTILITIES.INT_GLOBAL_DATA_REFRESH_DATE
), practices as (
 select distinct practice_code from REPORTING.OLIDS_ORGANISATION.DIM_PRACTICE
), later_dates as (
 select a.clinical_effective_date::date as event_date,a.publisher_organisation_code
 from DATA_LAKE.OLIDS.OBSERVATION a join practices p on a.publisher_organisation_code=p.practice_code
 where coalesce(a.lds_is_deleted,false)=false and a.person_id is not null
 and a.clinical_effective_date > (select refresh_date from baseline)
 and a.clinical_effective_date >= dateadd(month,-3,current_date()) and a.clinical_effective_date<current_date()
 union
 select a.clinical_effective_date::date,a.publisher_organisation_code
 from OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE a join practices p on a.publisher_organisation_code=p.practice_code
 where coalesce(a.lds_is_deleted,false)=false and a.person_id is not null
 and a.clinical_effective_date > (select refresh_date from baseline)
 and a.clinical_effective_date >= dateadd(month,-3,current_date()) and a.clinical_effective_date<current_date()
 union
 select a.clinical_effective_date::date,a.publisher_organisation_code
 from OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST a join practices p on a.publisher_organisation_code=p.practice_code
 where coalesce(a.lds_is_deleted,false)=false and a.person_id is not null
 and a.clinical_effective_date > (select refresh_date from baseline)
 and a.clinical_effective_date >= dateadd(month,-3,current_date()) and a.clinical_effective_date<current_date()
), consensus as (
 select event_date from later_dates group by event_date having count(distinct publisher_organisation_code)>=150
)
select (select refresh_date from baseline) as existing_refresh_date,
 coalesce(max(event_date),(select refresh_date from baseline)) as expanded_refresh_date,
 count(*) as later_dates_meeting_threshold from consensus;
