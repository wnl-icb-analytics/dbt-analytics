select *
from {{ ref('stg_mhsds_carecontact') }}
qualify row_number() over (
    partition by uniq_serv_req_id, uniq_care_cont_id
    order by
        reporting_period_end_date desc
        , effective_from desc
        , uniq_submission_id desc
        , mhs201_uniq_id desc
) = 1
