select
    r.cyp604_unique_id
    , r.local_patient_identifier_extended
    , r.blood_spot_card_completion_date::date as blood_spot_card_completion_date
    , r.newborn_blood_spot_test_result_received_date::date as blood_spot_result_received_date
    , r.newborn_blood_spot_test_outcome_status_code_phenylketonuria as outcome_phenylketonuria
    , r.newborn_blood_spot_test_outcome_status_code_sickle_cell_disease as outcome_sickle_cell_disease
    , r.newborn_blood_spot_test_outcome_status_code_cystic_fibrosis as outcome_cystic_fibrosis
    , r.newborn_blood_spot_test_outcome_status_code_congenital_hypothyroidism as outcome_congenital_hypothyroidism
    , r.newborn_blood_spot_test_outcome_status_code_medium_chain_acyl_coa_dehydrogenase_deficiency as outcome_medium_chain_acyl_coa_dehydrogenase_deficiency
    , r.newborn_blood_spot_test_outcome_status_code_homocystinuria as outcome_homocystinuria
    , r.newborn_blood_spot_test_outcome_status_code_maple_syrup_urine_disease as outcome_maple_syrup_urine_disease
    , r.newborn_blood_spot_test_outcome_status_code_glutaric_aciduria_type_1 as outcome_glutaric_aciduria_type_1
    , r.newborn_blood_spot_test_outcome_status_code_isovaleric_aciduria as outcome_isovaleric_aciduria
    , r.person_id
    , r.organisation_code_provider
    , r.organisation_identifier_code_of_provider
    , r.unique_submission_id
    , r.record_number
    , r.effective_from
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.file_type
    , h.csds_version
from {{ ref('raw_csds_cyp604bloodspotresult') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
