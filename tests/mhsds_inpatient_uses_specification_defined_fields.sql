with spell_expected as (
    select
        fact.admission_source_code
        , fact.admission_method_code
        , fact.planned_discharge_destination_code
        , fact.discharge_method_code
        , fact.discharge_destination_code
        , case
            when fact.mhsds_specification_version >= 5
                then source.source_adm_mh_hosp_prov_spell
            when fact.mhsds_specification_version < 5
                then source.source_adm_code_hosp_prov_spell
        end as expected_admission_source_code
        , case
            when fact.mhsds_specification_version >= 5
                then source.meth_adm_mh_hosp_prov_spell
            when fact.mhsds_specification_version < 5
                then source.adm_meth_code_hosp_prov_spell
        end as expected_admission_method_code
        , case
            when fact.mhsds_specification_version >= 5
                then source.planned_dest_disch
            when fact.mhsds_specification_version < 5
                then source.planned_disch_dest_code
        end as expected_planned_discharge_destination_code
        , case
            when fact.mhsds_specification_version >= 5
                then source.meth_of_disch_mh_hosp_prov_spell
            when fact.mhsds_specification_version < 5
                then source.disch_meth_code_hosp_prov_spell
        end as expected_discharge_method_code
        , case
            when fact.mhsds_specification_version >= 5
                then source.dest_of_disch_hosp_prov_spell
            when fact.mhsds_specification_version < 5
                then source.disch_dest_code_hosp_prov_spell
        end as expected_discharge_destination_code
    from {{ ref('fct_mhsds_hospital_provider_spell') }} as fact
    inner join {{ ref('stg_mhsds_spell') }} as source
        on fact.source_record_id = source.uniq_hosp_prov_spell_id
)

, spell_counts as (
    select
        count_if(admission_source_code
            is distinct from expected_admission_source_code)
            as admission_source
        , count_if(admission_method_code
            is distinct from expected_admission_method_code)
            as admission_method
        , count_if(planned_discharge_destination_code
            is distinct from expected_planned_discharge_destination_code)
            as planned_discharge_destination
        , count_if(discharge_method_code
            is distinct from expected_discharge_method_code)
            as discharge_method
        , count_if(discharge_destination_code
            is distinct from expected_discharge_destination_code)
            as discharge_destination
    from spell_expected
)

, spell_failures as (
    select
        'hospital_provider_spell' as model_name
        , field_name
        , failure_count
    from spell_counts
    unpivot (
        failure_count for field_name in (
            admission_source
            , admission_method
            , planned_discharge_destination
            , discharge_method
            , discharge_destination
        )
    )
    where failure_count > 0
)

, ward_expected as (
    select
        fact.ward_site_code
        , fact.ward_setting_code
        , fact.ward_intended_age_group_code
        , fact.ward_intended_sex_code
        , fact.ward_clinical_care_intensity_code
        , fact.ward_security_level_code
        , fact.locked_ward_indicator_code
        , case
            when fact.mhsds_specification_version >= 6
                then details.site_id_of_ward
            when fact.mhsds_specification_version < 6
                then source.site_id_of_treat
        end as expected_ward_site_code
        , case
            when fact.mhsds_specification_version >= 6 then details.ward_type
            when fact.mhsds_specification_version < 6 then source.ward_type
        end as expected_ward_setting_code
        , case
            when fact.mhsds_specification_version >= 6 then details.ward_age
            when fact.mhsds_specification_version < 6 then source.ward_age
        end as expected_ward_intended_age_group_code
        , case
            when fact.mhsds_specification_version >= 6
                then details.ward_intended_sex
            when fact.mhsds_specification_version < 6
                then source.ward_sex_type_code
        end as expected_ward_intended_sex_code
        , case
            when fact.mhsds_specification_version >= 6
                then details.ward_intended_clin_care_mh
            when fact.mhsds_specification_version < 6
                then source.intend_clin_care_inten_code_mh
        end as expected_ward_clinical_care_intensity_code
        , case
            when fact.mhsds_specification_version >= 6
                then details.ward_sec_level
            when fact.mhsds_specification_version < 6 then source.ward_sec_level
        end as expected_ward_security_level_code
        , case lower(trim(
            case
                when fact.mhsds_specification_version >= 6
                    then details.locked_ward_ind::varchar
                when fact.mhsds_specification_version < 6
                    then source.locked_ward_ind::varchar
            end
        ))
            when 'true' then 'Y'
            when 'false' then 'N'
            when '1' then 'Y'
            when '0' then 'N'
            when 'y' then 'Y'
            when 'n' then 'N'
            else upper(trim(
                case
                    when fact.mhsds_specification_version >= 6
                        then details.locked_ward_ind::varchar
                    when fact.mhsds_specification_version < 6
                        then source.locked_ward_ind::varchar
                end
            ))
        end as expected_locked_ward_indicator_code
    from {{ ref('fct_mhsds_ward_stay') }} as fact
    inner join {{ ref('stg_mhsds_mhs502wardstay') }} as source
        on fact.source_record_id = source.uniq_ward_stay_id
    left join {{ ref('stg_mhsds_mhs903warddetails') }} as details
        on source.uniq_submission_id = details.uniq_submission_id
        and source.uniq_ward_code = details.uniq_ward_code
)

, ward_counts as (
    select
        count_if(ward_site_code is distinct from expected_ward_site_code)
            as ward_site
        , count_if(ward_setting_code is distinct from expected_ward_setting_code)
            as ward_setting
        , count_if(ward_intended_age_group_code
            is distinct from expected_ward_intended_age_group_code)
            as ward_intended_age_group
        , count_if(ward_intended_sex_code
            is distinct from expected_ward_intended_sex_code)
            as ward_intended_sex
        , count_if(ward_clinical_care_intensity_code
            is distinct from expected_ward_clinical_care_intensity_code)
            as ward_clinical_care_intensity
        , count_if(ward_security_level_code
            is distinct from expected_ward_security_level_code)
            as ward_security_level
        , count_if(locked_ward_indicator_code
            is distinct from expected_locked_ward_indicator_code)
            as locked_ward_indicator
    from ward_expected
)

, ward_failures as (
    select
        'ward_stay' as model_name
        , field_name
        , failure_count
    from ward_counts
    unpivot (
        failure_count for field_name in (
            ward_site
            , ward_setting
            , ward_intended_age_group
            , ward_intended_sex
            , ward_clinical_care_intensity
            , ward_security_level
            , locked_ward_indicator
        )
    )
    where failure_count > 0
)

select * from spell_failures
union all
select * from ward_failures
