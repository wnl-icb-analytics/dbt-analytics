{#-
    Reporting date for the segmentation activity windows. Every 12-month
    window in the current segmentation models ends on this date, so all
    sources cover the same period in a run.

    Defaults to the last completed month-end (London time) before the run
    started. It is taken from run_started_at, so every model in one invocation
    gets the same date. Override with
    --vars '{segmentation_reporting_date: "YYYY-MM-DD"}'.
-#}
{% macro segmentation_reporting_date() -%}
    {%- set override = var('segmentation_reporting_date', none) -%}
    {%- if override -%}
        {%- set parsed = modules.datetime.datetime.strptime(override | string, '%Y-%m-%d') -%}
        '{{ parsed.strftime('%Y-%m-%d') }}'::DATE
    {%- else -%}
        {%- set london_time = run_started_at.astimezone(modules.pytz.timezone('Europe/London')) -%}
        {%- set month_end = london_time.replace(day=1) - modules.datetime.timedelta(days=1) -%}
        '{{ month_end.strftime('%Y-%m-%d') }}'::DATE
    {%- endif -%}
{%- endmacro %}
