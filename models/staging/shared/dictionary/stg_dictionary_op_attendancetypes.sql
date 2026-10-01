select
    sk_attendance_type,
    bk_attendance_type_code,
    attendant_type,
    attendant_type_desc,
    date_created,
    date_updated
from {{ ref('raw_dictionary_op_attendancetypes') }}