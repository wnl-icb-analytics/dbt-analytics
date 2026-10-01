select
    sk_priority_type_id,
    bk_priority_type_code,
    priority_type_desc,
    date_created,
    date_updated
from {{ ref('raw_dictionary_op_prioritytype') }}