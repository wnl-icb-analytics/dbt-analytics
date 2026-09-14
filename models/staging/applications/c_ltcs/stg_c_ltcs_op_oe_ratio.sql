select
    {{ consistent_sk_patient_id_format('patient_id')}}  as patient_id,
    oe_ratio,
    predicted,
    op_att_tot_12_mo
from {{ ref('raw_c_ltcs_op_oe_ratio') }}
