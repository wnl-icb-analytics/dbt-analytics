{{ config(materialized='view', tags=['person_clinical_record', 'daily']) }}

-- Reads the attendance once and emits one row per recorded single-valued clinical field.
with item_type as (
    select column1 as source_record_type, column2 as is_injury_detail
    from values
        ('chief_complaint', false),
        ('acuity', false),
        ('notifiable_disease', false),
        ('injury_mechanism', true),
        ('injury_intent', true),
        ('injury_place', true)
),

attendance_item as (
    select
        a.visit_occurrence_id
        , t.source_record_type
        , case t.source_record_type
            when 'chief_complaint' then a.chief_complaint_code
            when 'acuity' then a.acuity
            when 'notifiable_disease' then a.disease_notification_code
            when 'injury_mechanism' then a.injury_mechanism_code
            when 'injury_intent' then a.injury_intent_code
            when 'injury_place' then a.place_of_injury_code
        end::varchar as item_code
        , case t.source_record_type
            when 'chief_complaint' then a.chief_complaint_desc
            when 'acuity' then a.acuity_desc
            when 'notifiable_disease' then a.disease_notification_desc
            when 'injury_mechanism' then a.injury_mechanism_desc
            when 'injury_intent' then a.injury_intent_desc
            when 'injury_place' then a.place_of_injury_desc
        end::varchar as item_name
        -- Injury details take the recorded injury date; the other fields have no supported clinical date.
        , iff(t.is_injury_detail, a.injury_date, null)::date as item_date
        , iff(t.is_injury_detail, a.injury_time, null)::time as item_time
    from {{ ref('obt_encounter_uec') }} as a
    cross join item_type as t
)

select
    visit_occurrence_id
    , source_record_type
    , visit_occurrence_id::varchar as source_record_id
    , item_code
    , item_name
    , item_date
    , item_time
from attendance_item
where item_code is not null
