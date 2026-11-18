/*
COVID Campaign Selection

covid_reported_campaign_ids() is the set of campaigns the COVID models build, and
covid_reported_campaigns() emits them as one CTE:

  WITH all_campaigns AS (
      {{ covid_reported_campaigns() }}
  )

Every campaign listed there stays in the models for good, so a season that has been
reported keeps its rows when the next one is added, and its population is resolved as at
the campaign rather than as at today (int_covid_flu_campaign_population). Clinical
eligibility is still recomputed on every build, so a code entered retrospectively can
still move a closed season.

To add a season, define it in covid_campaign_config() first, then append its id here.

covid_current_autumn() and its siblings name the season in flight. They do not control
which campaigns are built; they exist so a model that needs "this season" can say so
rather than hardcoding an id. covid_current_spring() names COVID Spring 2027 even though
that campaign is not published yet, so that it is ready when UKHSA authorises the offer.

Dates live in covid_campaign_config() only. This file holds no dates.
*/

{# ===== Campaigns the models build (add new campaigns here; never remove) ===== #}

{% macro covid_reported_campaign_ids() %}
    {{ return([
        'COVID Autumn 2024',
        'COVID Spring 2025',
        'COVID Autumn 2025',
        'COVID Spring 2026',
        'COVID Autumn 2026'
    ]) }}
    {#- COVID Spring 2027 is configured in covid_campaign_config() but deliberately not
        listed. Spec v4.0 supplies the spring dates but defines a call and recall group
        for Autumn 2026 only, so there is no authorised spring offer to publish an
        eligible cohort against. Add it here once UKHSA defines one. #}
{% endmacro %}

{% macro covid_reported_campaigns() %}
    {%- for campaign_id in covid_reported_campaign_ids() %}
    {%- if not loop.first %}
    UNION ALL
    {% endif %}
    SELECT * FROM ({{ covid_campaign_config(campaign_id) }})
    {%- endfor %}
{% endmacro %}

{# ===== Season in flight (roll these forward each year) ===== #}

{% macro covid_current_autumn() %}COVID Autumn 2026{% endmacro %}
{% macro covid_current_spring() %}COVID Spring 2027{% endmacro %}
{% macro covid_previous_autumn() %}COVID Autumn 2025{% endmacro %}
{% macro covid_previous_spring() %}COVID Spring 2026{% endmacro %}

{# ===== Single-campaign config accessors ===== #}

{% macro covid_autumn_config() %}{{ covid_campaign_config(covid_current_autumn()) }}{% endmacro %}
{% macro covid_spring_config() %}{{ covid_campaign_config(covid_current_spring()) }}{% endmacro %}
{% macro covid_previous_autumn_config() %}{{ covid_campaign_config(covid_previous_autumn()) }}{% endmacro %}
{% macro covid_previous_spring_config() %}{{ covid_campaign_config(covid_previous_spring()) }}{% endmacro %}
