-- Fixed leap-year reference distinguishes calendar months from day windows.
with inputs as (
    select column1::varchar id, column2::date order_date
    from values ('SYN_BOUNDARY','2024-02-29'),('SYN_INSIDE','2024-03-01'),
        ('SYN_TODAY','2024-03-31'),('SYN_FUTURE','2024-04-01'),('SYN_NULL',null)
), actual as (
    select id, {{ medication_date_flags('order_date', months=[1,3,6,12], reference_date="'2024-03-31'::date") }}
    from inputs
), expected as (
    select column1::varchar id, column2::boolean is_future_dated,
        column3::boolean is_recent_1m, column4::boolean is_recent_3m,
        column4::boolean is_recent_6m, column4::boolean is_recent_12m
    from values ('SYN_BOUNDARY',false,false,true),('SYN_INSIDE',false,true,true),
        ('SYN_TODAY',false,true,true),('SYN_FUTURE',true,false,false),('SYN_NULL',false,false,false)
)
(select * from actual minus select * from expected)
union all
(select * from expected minus select * from actual)
