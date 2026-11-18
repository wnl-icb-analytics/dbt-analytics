{% docs ltc_register_by_month_scope %}
One row per person on the register at a completed month-end, for people registered and alive on that date. Every month applies today's register rules and code lists, not the rules in force at the time, and the whole history is recomputed each month, so past months can change as records are added or corrected. Covers the last 60 completed month-ends: records for people who left or died are kept for five years, so earlier months would under-count. A diagnosis, observation or prescription counts from the month-end on or after both its clinical (or order) date and its recorded date, so a diagnosis coded late appears from when it was entered; a record with no recorded date counts from its clinical date. Rebuilt in the monthly full refresh.
{% enddocs %}

{% docs ltc_register_by_month_month_end_date %}
Completed month-end at which register membership is evaluated.
{% enddocs %}

{% docs ltc_register_by_month_practice_code %}
ODS code of the practice the person was registered with at the month-end. Where two registrations overlap, the one still open today is preferred, so a past month can show a later practice.
{% enddocs %}
