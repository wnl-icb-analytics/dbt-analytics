select
    attr_name,
    valid_from,
    valid_to,
    main_code_text,
    sub_code1_text,
    sub_code2_text,
    sub_code3_text,
    major_category,
    category,
    main_description,
    main_description_60_chars,
    tg.treatment_group,
    sub1_description,
    sub2_description,
    sub3_description,
    notes,
    in_source_table,
    unique_column,
    import_date,
    created_date,
    is_latest,
    effective_from,
    effective_to
from {{ ref('raw_ukhfd_data_dictionary_cancer_treatment_modality') }} ctm

left join {{ ref('cwt_cancer_treatment_modality_group')}} tg
on ctm.main_code_text = tg.code

where is_latest = 1