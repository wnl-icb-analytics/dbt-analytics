with source as (
    select
        *,
        -- File names carry the release, e.g. ..._ECDS_ETOS_v4.0.8_UnmergedCells.xlsx.
        regexp_substr(file_name, '[-_]v([0-9]+(\\.[0-9]+)*)', 1, 1, 'ie', 1) as version_number,
        file_name ilike '%-draft%' as is_draft_release
    from {{ ref('raw_ukhfd_ecds_etos_code_sets') }}
)

select
    version_number || iff(is_draft_release, '-draft', '') as etos_version,
    -- Missing version parts count as 0, so v4 sorts as 4.0.0.0.
    split_part(version_number, '.', 1)::int * 1000000
        + coalesce(try_to_number(split_part(version_number, '.', 2)), 0) * 10000
        + coalesce(try_to_number(split_part(version_number, '.', 3)), 0) * 100
        + coalesce(try_to_number(split_part(version_number, '.', 4)), 0)
        as etos_version_sort_key,
    is_draft_release,
    cast(created_date as date) as etos_release_date,
    trim(file_name) as source_file_name,
    regexp_substr(trim(sheet_name), '^[0-9.]+') as code_set_number,
    trim(regexp_replace(trim(sheet_name), '^[0-9.]+\\s*', '')) as code_set_name,
    trim(snomed_code) as snomed_code,
    trim(ecds_unique_id) = 'Code deprecated' as is_deprecated,

    {{ clean_ecds_etos_text('ecds_unique_id') }} as ecds_unique_id,
    {{ clean_ecds_etos_text('refset_unique_id') }} as refset_unique_id,
    {{ clean_ecds_etos_text('sort1') }} as sort1,
    {{ clean_ecds_etos_text('sort2') }} as sort2,
    {{ clean_ecds_etos_text('sort3') }} as sort3,
    {{ clean_ecds_etos_text('sort4') }} as sort4,
    {{ clean_ecds_etos_text('ecds_group1') }} as ecds_group1,
    {{ clean_ecds_etos_text('ecds_group2') }} as ecds_group2,
    {{ clean_ecds_etos_text('ecds_group3') }} as ecds_group3,
    {{ clean_ecds_etos_text('ecds_description') }} as ecds_description,
    {{ clean_ecds_etos_text('ecds_search_terms') }} as ecds_search_terms,
    {{ clean_ecds_etos_text('snomed_uk_preferred_term') }} as snomed_uk_preferred_term,
    {{ clean_ecds_etos_text('snomed_fully_specified_name') }} as snomed_fully_specified_name,
    -- Releases before v3.1.0 publish the SNOMED name in these two fields instead.
    {{ clean_ecds_etos_text('snomed_description') }} as snomed_description,
    {{ clean_ecds_etos_text('snomed_term') }} as snomed_term,
    {{ clean_ecds_etos_text('parent_term') }} as parent_snomed_code,
    maps_to_self,

    -- ETOS renamed Inj to Injury and AEC to SDEC; no row populates both.
    {{ ecds_etos_flag('coalesce(flag_injury, flag_inj)') }} as is_injury,
    {{ ecds_etos_flag('coalesce(flag_sdec, flag_aec)') }} as is_same_day_emergency_care,
    {{ ecds_etos_flag('flag_allergy') }} as is_allergy,
    {{ ecds_etos_flag('flag_notifiable_disease') }} as is_notifiable_disease,
    {{ ecds_etos_flag('flag_male') }} as is_valid_for_male,
    {{ ecds_etos_flag('flag_female') }} as is_valid_for_female,
    {{ ecds_etos_flag('flag_ads') }} as is_ads,
    {{ ecds_etos_flag('flag_pain') }} as is_pain,
    {{ ecds_etos_flag('chief_complaint_core_list') }} as is_chief_complaint_core_list,
    {{ ecds_etos_flag('chief_complaint_max_list') }} as is_chief_complaint_max_list,
    {{ clean_ecds_etos_text('core_list_term') }} as core_list_snomed_code,
    {{ clean_ecds_etos_text('core_list_term_description') }} as core_list_term_description,

    -- Older releases omit the ICD-10 dot (K589); one row holds an ICD-11 code, left as published.
    case
        when regexp_like({{ clean_ecds_etos_text('icd10_mapping') }}, '[A-Z][0-9].*')
            then {{ clean_icd10_code(clean_ecds_etos_text('icd10_mapping')) }}
        else {{ clean_ecds_etos_text('icd10_mapping') }}
    end as icd10_mapping,
    {{ clean_ecds_etos_text('icd10_description') }} as icd10_description,
    {{ clean_ecds_etos_text('icd11_mapping') }} as icd11_mapping,
    {{ clean_ecds_etos_text('icd11_description') }} as icd11_description,
    {{ clean_ecds_etos_text('icd10_code') }} as comorbidity_icd10_codes,
    {{ ecds_etos_flag('charlson') }} as is_charlson_comorbidity,
    {{ ecds_etos_flag('elixhauser') }} as is_elixhauser_comorbidity,
    {{ clean_ecds_etos_text('cds10_mapping_diagnosis_condition') }} as cds10_mapping_diagnosis_condition,
    {{ clean_ecds_etos_text('cds10_mapping_sub_analysis_code') }} as cds10_mapping_sub_analysis_code,
    {{ clean_ecds_etos_text('cds10_mapping_anatomical_area') }} as cds10_mapping_anatomical_area,
    {{ clean_ecds_etos_text('cds_code_mapping_used_for_hrg_grouping') }} as cds_code_mapping_used_for_hrg_grouping,
    {{ clean_ecds_etos_text('cds_investigation_mapping_that_is_used_for_hrg_grouping') }} as cds_investigation_mapping_used_for_hrg_grouping,
    {{ clean_ecds_etos_text('cds_treatment_mapping_that_is_used_for_hrg_grouping') }} as cds_treatment_mapping_used_for_hrg_grouping,
    -- A few values were converted to dates by Excel (e.g. 02/01/2021); left as published.
    {{ clean_ecds_etos_text('pb_r_category') }} as pbr_category,

    {{ clean_ecds_etos_text('notes') }} as notes,
    {{ clean_ecds_etos_text('ecds_notes') }} as ecds_notes,
    {{ clean_ecds_etos_text('notes_for_completing_the_observation_value_field') }} as observation_value_notes,
    valid_from as valid_from_date,
    valid_to as valid_to_date,
    unique_column as source_unique_key,
    import_date as source_imported_at
from source
