/*
Pregnancy / HDP window configuration — general purpose

Tuneables for the reusable pregnancy episode and HDP exclusion window models
in models/modelling/olids/person_attributes/. Override per cohort if needed.
*/

{% macro pregnancy_episode_max_weeks() %}
    {{ var('pregnancy_episode_max_weeks', 42) }}
{% endmacro %}

{% macro hdp_postpartum_extension_weeks() %}
    {{ var('hdp_postpartum_extension_weeks', 8) }}
{% endmacro %}
