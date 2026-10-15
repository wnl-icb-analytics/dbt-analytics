{% test iapt_semantic_waits(model) %}

-- NHS England waits use supplied referral dates; local chain waits contribute once under the original provider.
{% set waits = [
    ('first_assessment', 'referral_received_date', 'first_assessment_date', 'first_assessment_count',
        'median_days_to_first_assessment', [
            ('first_assessment_within_28_days_count', '< 29'),
            ('first_assessment_29_to_56_days_count', 'between 29 and 56'),
            ('first_assessment_57_to_90_days_count', 'between 57 and 90'),
            ('first_assessment_over_90_days_count', '> 90')]),
    ('first_treatment', 'referral_received_date', 'first_treatment_date', 'first_treatment_count',
        'median_days_to_first_treatment', [
            ('first_treatment_within_6_weeks_count', '<= 42'),
            ('first_treatment_within_18_weeks_count', '<= 126')]),
    ('second_treatment', 'first_treatment_date', 'second_treatment_date', 'first_to_second_treatment_count',
        'median_days_first_to_second_treatment', [
            ('first_to_second_treatment_over_28_days_count', '> 28'),
            ('first_to_second_treatment_over_90_days_count', '> 90')])
] %}

{% for chain in [false, true] %}
{% for stage, start_date, end_date, count_metric, median_metric, bands in waits %}
{% set metric_prefix = 'pathway_' if chain else '' %}
{% set start_field = 'pathway_' ~ start_date if chain and start_date != 'referral_received_date' else start_date %}
{% set end_field = metric_prefix ~ end_date %}
{% set month_dimension = ('pathway_' if chain else 'referral_') ~ stage ~ '_month' %}
select '{{ 'pathway' if chain else 'nhse' }}_{{ stage }}' as measure_population,
    count(*) as failing_provider_month_count
from (
    select
        provider_organisation_code
        , date_trunc('month', {{ end_field }}) as wait_month
        , count(datediff(day, {{ start_field }}, {{ end_field }})) as wait_count
        {% for band_metric, condition in bands %}
        , count_if(datediff(day, {{ start_field }}, {{ end_field }}) {{ condition }}) as band_{{ loop.index }}_count
        {% endfor %}
        , median(datediff(day, {{ start_field }}, {{ end_field }})) as median_days
    from {{ ref('fct_iapt_referral_summary') }}
    where {{ 'referral_id = original_referral_id' if chain else 'is_nhse_use_pathway' }}
    {% if not chain %}
        and (not is_transfer_successor
            or {{ end_field }} >= date_trunc('month', first_reported_period_end_date))
    {% endif %}
    group by provider_organisation_code, date_trunc('month', {{ end_field }})
) as e
full outer join (
    select *
    from semantic_view(
        {{ model }}
        metrics referrals.{{ metric_prefix }}{{ count_metric }},
            {% for band_metric, condition in bands %}
            referrals.{{ metric_prefix }}{{ band_metric }},
            {% endfor %}
            referrals.{{ metric_prefix }}{{ median_metric }}
        dimensions referrals.referrals_provider_code, referrals.{{ month_dimension }}
            {% if not chain %}, referrals.referrals_is_nhse_use_pathway{% endif %}
    )
    {% if not chain %}where referrals_is_nhse_use_pathway{% endif %}
) as a
    on e.provider_organisation_code is not distinct from a.referrals_provider_code
    and e.wait_month is not distinct from a.{{ month_dimension }}
where coalesce(e.wait_count, 0) <> coalesce(a.{{ metric_prefix }}{{ count_metric }}, 0)
    {% for band_metric, condition in bands %}
    or coalesce(e.band_{{ loop.index }}_count, 0) <> coalesce(a.{{ metric_prefix }}{{ band_metric }}, 0)
    {% endfor %}
    or e.median_days is distinct from a.{{ metric_prefix }}{{ median_metric }}
having count(*) > 0
{% if not (chain and loop.last) %}union all{% endif %}
{% endfor %}
{% endfor %}

{% endtest %}
