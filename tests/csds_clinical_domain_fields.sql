select f.source_record_id, f.source_table, f.clinical_record_type
from {{ ref('fct_csds_clinical_record') }} as f
left join {{ ref('stg_csds_blood_spot_result') }} as blood_spot
    on f.source_table = 'CYP604' and f.source_row_id = blood_spot.cyp604_unique_id::varchar
left join {{ ref('stg_csds_activity_assessment') }} as assessment
    on f.source_table = 'CYP612' and f.source_row_id = assessment.cyp612_unique_id::varchar
where f.blood_spot_result_received_date is distinct from blood_spot.blood_spot_result_received_date
    or (f.source_table = 'CYP604' and f.clinical_date is distinct from blood_spot.blood_spot_card_completion_date)
    or f.asq_score_band is distinct from assessment.asq_score_band::varchar
