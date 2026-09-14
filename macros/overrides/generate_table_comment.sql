{% macro generate_table_comment() %}
  {%- if execute -%}
    {%- set model_description = model.description or config.get('description') or "" -%}
    {%- set current_user = target.user | default('SYSTEM') -%}

    {#- Convert UTC to Europe/London timezone -#}
    {%- set pytz = modules.pytz -%}
    {%- set london_tz = pytz.timezone('Europe/London') -%}
    {%- set london_time = run_started_at.astimezone(london_tz) -%}

    {%- set day = london_time.strftime('%d')|int -%}
    {%- set day_suffix = 'th' -%}
    {%- if day % 10 == 1 and day != 11 -%}{%- set day_suffix = 'st' -%}{%- endif -%}
    {%- if day % 10 == 2 and day != 12 -%}{%- set day_suffix = 'nd' -%}{%- endif -%}
    {%- if day % 10 == 3 and day != 13 -%}{%- set day_suffix = 'rd' -%}{%- endif -%}
    {%- set tz_abbr = london_time.strftime('%Z') -%}
    {%- set run_timestamp = day|string + day_suffix + london_time.strftime(' %B %Y at %H:%M:%S ') + tz_abbr -%}
    {%- set target_name = target.name | default('unknown') -%}

    {%- set github_base_url = "https://github.com/wnl-icb-analytics/dbt-analytics/blob/main/" -%}
    {%- set model_file_path = model.original_file_path | replace("\\", "/") -%}
    {%- set github_file_url = github_base_url + model_file_path -%}
    {%- set model_source_line = '' -%}
    {%- if target_name in ['prod', 'snowflake-prod', 'ci-prod'] -%}
      {%- set model_source_line = "\n📄 Model source: " + github_file_url -%}
    {%- endif -%}

    {#- Check for custom message in meta config -#}
    {%- set model_meta = config.get('meta', {}) -%}
    {%- set custom_message = model_meta.get('custom_message', '') -%}
    {%- if custom_message -%}
      {%- set custom_message = "⚠️ " + custom_message + "\n\n" -%}
    {%- endif -%}

    {#- Get owner from meta config -#}
    {%- set owner = model_meta.get('owner', {}) -%}
    {%- set owner_name = owner.get('name', '') if owner else '' -%}
    {%- set owner_line = '' -%}
    {%- if owner_name -%}
      {%- set owner_line = "
👤 Owner: " + owner_name -%}
    {%- endif -%}

    {%- if model_description or custom_message -%}
      {%- set clean_description = (custom_message + model_description) | replace("'", "''") -%}
      {%- set footer = owner_line + "
🤖 Last ran on " + run_timestamp + " by " + current_user + " (target: " + target_name + ")" + model_source_line + "
📖 Documentation: https://github.com/wnl-icb-analytics/dbt-analytics" -%}
      {{- clean_description + footer | replace("'", "''") -}}
    {%- else -%}
{{- owner_line -}}
🤖 Last ran on {{ run_timestamp }} by {{ current_user }} (target: {{ target_name }}){{ model_source_line }}
📖 Documentation: https://github.com/wnl-icb-analytics/dbt-analytics
    {%- endif -%}
  {%- endif -%}
{% endmacro %}
