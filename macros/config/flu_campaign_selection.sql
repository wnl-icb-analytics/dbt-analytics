/*
Flu Campaign Selection

flu_reported_campaign_ids() is the set of campaigns the flu models build, and
flu_reported_campaigns() emits them as one CTE:

  WITH all_campaigns AS (
      {{ flu_reported_campaigns() }}
  )

Every campaign listed there stays in the models for good, so a season that has been
reported keeps its rows when the next one is added, and its population is resolved as at
the campaign rather than as at today (int_covid_flu_campaign_population). Clinical
eligibility is still recomputed on every build, so a code entered retrospectively can
still move a closed season.

To add a season, define it in flu_campaign_config() first, then append its id here.

flu_current_campaign() and flu_previous_campaign() name the season in flight. They do not
control which campaigns are built; they exist so a model that needs "this season" can say
so rather than hardcoding an id.

Dates live in flu_campaign_config() only. This file holds no dates.
*/

{# ===== Campaigns the models build (add new campaigns here; never remove) ===== #}

{% macro flu_reported_campaign_ids() %}
    {{ return([
        'Flu 2024-25',
        'Flu 2025-26',
        'Flu 2026-27'
    ]) }}
{% endmacro %}

{% macro flu_reported_campaigns() %}
    {%- for campaign_id in flu_reported_campaign_ids() %}
    {%- if not loop.first %}
    UNION ALL
    {% endif %}
    SELECT * FROM ({{ flu_campaign_config(campaign_id) }})
    {%- endfor %}
{% endmacro %}

{# ===== Season in flight (roll these forward each year) ===== #}

{% macro flu_current_campaign() %}Flu 2026-27{% endmacro %}
{% macro flu_previous_campaign() %}Flu 2025-26{% endmacro %}

{# ===== Single-campaign config accessors ===== #}

{% macro flu_current_config() %}{{ flu_campaign_config(flu_current_campaign()) }}{% endmacro %}
{% macro flu_previous_config() %}{{ flu_campaign_config(flu_previous_campaign()) }}{% endmacro %}

{# ===== Campaigns a build recomputes ===== #}

{#- Closed seasons are recomputed monthly, not daily. The cohort intermediates are
    incremental (delete+insert on campaign_id) and take their all_campaigns CTE from
    this macro: a routine incremental run rebuilds only the season in flight, so a
    closed season keeps its rows until a run with --full-refresh, or with
    --vars '{covid_flu_rebuild_closed: true}', restates every campaign against the
    current source data and its pinned terminology version. -#}

{% macro flu_build_campaigns() %}
    {%- if is_incremental() and not var('covid_flu_rebuild_closed', false) -%}
    SELECT * FROM ({{ flu_reported_campaigns() }})
    WHERE campaign_id = '{{ flu_current_campaign() }}'
    {%- else -%}
    {{ flu_reported_campaigns() }}
    {%- endif -%}
{% endmacro %}
