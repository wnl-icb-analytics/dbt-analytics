select
    sk_attendance_outcome,
    bk_attendance_outcome,
    attendance_outcome,
    date_created,
    date_updated
from {{ ref('raw_dictionary_op_attendanceoutcomes') }}