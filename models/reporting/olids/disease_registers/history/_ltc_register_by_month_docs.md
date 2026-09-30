{% docs ltc_register_by_month_scope %}
One row per person on the register at a completed month-end, for people registered and alive on that date. Covers the last 60 completed month-ends: records for people who left or died are kept for five years, so earlier months would under-count. A diagnosis or observation counts from the month-end on or after both its clinical date and its recorded date, so a diagnosis coded late appears from when it was entered; medication windows use the order date. Rebuilt in the monthly full refresh.
{% enddocs %}

{% docs ltc_register_by_month_month_end_date %}
Completed month-end at which register membership is evaluated.
{% enddocs %}

{% docs ltc_register_by_month_practice_code %}
ODS code of the practice the person was registered with at the month-end.
{% enddocs %}
