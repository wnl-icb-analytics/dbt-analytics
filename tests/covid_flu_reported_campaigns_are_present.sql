-- Every campaign the models are configured to report must reach the combined uptake fact.
--
-- The campaign set is append-only: covid_reported_campaign_ids() and
-- flu_reported_campaign_ids() are the contract that a season already reported keeps its
-- rows when the next one is added. Nothing else enforces that. A campaign can drop out
-- silently if its config branch stops matching, if an eligibility flag switches every
-- cohort off, or if a downstream filter excludes it.

WITH expected AS (
    {%- for campaign_id in covid_reported_campaign_ids() %}
    SELECT 'COVID' AS programme_type, '{{ campaign_id }}' AS campaign_id
    UNION ALL
    {%- endfor %}
    {%- for campaign_id in flu_reported_campaign_ids() %}
    SELECT 'FLU' AS programme_type, '{{ campaign_id }}' AS campaign_id
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {%- endfor %}
),

actual AS (
    SELECT DISTINCT programme_type, campaign_id
    FROM {{ ref('fct_covid_flu_uptake') }}
)

SELECT
    e.programme_type,
    e.campaign_id,
    'Reported campaign is missing from fct_covid_flu_uptake' AS failure_reason
FROM expected e
LEFT JOIN actual a
    ON a.programme_type = e.programme_type
    AND a.campaign_id = e.campaign_id
WHERE a.campaign_id IS NULL
