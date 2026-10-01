-- Staging model for Fact Patient Practice registrations
-- Source: DATA_LAKE.FACT_PATIENT.FactPractice
-- More reliable than PDS for GP registration data

select
    raw.sk_patient_id,
    raw.sk_organisation_id,
    org.organisation_code as practice_ods_code,
    org.organisation_name as practice_name,
    raw.period_start,
    raw.period_end,
    raw.date_detected_join,
    raw.date_detected_left,
    raw.sk_data_source_id,

    -- Flag for current registration
    case
        when raw.period_end is null or raw.period_end >= current_date()
        then true
        else false
    end as is_current_registration

from {{ ref('raw_fact_patient_factpractice') }} raw
inner join {{ ref('stg_dictionary_dbo_organisation') }} org
    on raw.sk_organisation_id = org.sk_organisation_id
where raw.sk_patient_id is not null
    and raw.sk_organisation_id is not null

-- Deduplicate: keep most recent registration per patient-practice combination
qualify row_number() over (
    partition by raw.sk_patient_id, raw.sk_organisation_id, raw.period_start
    order by raw.date_detected_join desc nulls last
) = 1
