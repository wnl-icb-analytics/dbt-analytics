with identified as (
    select s.*, {{ dbt_utils.generate_surrogate_key(['s.org_id_prov', 's.uniq_hosp_prov_spell_id', 's.start_date_org_code_comm', 's.org_id_comm', 'iff(s.uniq_hosp_prov_spell_id is null or s.start_date_org_code_comm is null or s.org_id_comm is null, s.mhs512_uniq_id::varchar, null)']) }} as entity_id
        , s.uniq_hosp_prov_spell_id is null or s.start_date_org_code_comm is null or s.org_id_comm is null as is_identity_incomplete
    from {{ ref('stg_mhsds_spell_commissioner_period') }} as s
), selected as (
    select *
        , count(*) over (partition by entity_id) as n_accepted_source_records
        , min(reporting_period_end_date) over (partition by entity_id) as first_submission_period_end_date
    from identified
    qualify row_number() over (partition by entity_id order by reporting_period_end_date desc, effective_from desc nulls last, uniq_submission_id desc, row_number desc nulls last, mhs512_uniq_id desc) = 1
)
select
    s.entity_id as spell_commissioner_period_id
    , s.mhs512_uniq_id as source_row_id
    , 'MHS512' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.uniq_hosp_prov_spell_id as hospital_provider_spell_source_record_id
    , s.org_id_comm as commissioner_organisation_code
    , s.start_date_org_code_comm as commissioner_start_date
    , s.end_date_org_code_comm as commissioner_end_date
    , s.dm_icb_commissioner as source_derived_icb_commissioner_code
    , s.dm_sub_icb_commissioner as source_derived_sub_icb_commissioner_code
    , commissioner.organisation_name as commissioner_organisation_name
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
    , s.n_accepted_source_records
    , s.first_submission_period_end_date
    , s.is_identity_incomplete
    , icb.organisation_name as source_derived_icb_commissioner_name
    , subicb.organisation_name as source_derived_sub_icb_commissioner_name
from selected as s
left join {{ ref('stg_mhsds_bridging') }} as b on s.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as commissioner on upper(s.org_id_comm) = upper(commissioner.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as icb
    on upper(s.dm_icb_commissioner) = upper(icb.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as subicb
    on upper(s.dm_sub_icb_commissioner) = upper(subicb.organisation_code)
