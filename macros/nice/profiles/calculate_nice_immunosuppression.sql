{% macro calculate_nice_immunosuppression(reference='current') %}
{#-
    Reconstruct the existing COVID immunosuppression proxy for the NICE shingles cohort.
    Args: reference is current or by_month.
    Returns: person_id, reporting_date and latest_evidence_date for proxy members
             who reached 75 in (R-12 months,R], active and non-test at R.
    The current autumn campaign supplies today's terminology and the audit-end cap.
-#}
-- NICE IND219 immunocompromise proxy. This is not an authorised shingles-specific definition.
WITH current_campaign AS (
    {{ covid_autumn_config() }}
),

candidates AS (
    SELECT
        population.person_id,
        population.reporting_date,
        LEAST(population.reporting_date, campaign.audit_end_date) AS proxy_reference_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    CROSS JOIN current_campaign AS campaign
    WHERE DATEADD(year, 75, population.birth_date_approx) > DATEADD(month, -12, population.reporting_date)
        AND DATEADD(year, 75, population.birth_date_approx) <= population.reporting_date
),

candidate_keys AS (
    SELECT DISTINCT person_id
    FROM candidates
),

daily_evidence AS (
    SELECT
        evidence.person_id,
        evidence.evidence_type,
        -- Preserve the campaign proxy's timestamp <= midnight at the reference date.
        IFF(evidence.evidence_date = evidence.evidence_date::DATE,
            evidence.evidence_date::DATE,
            DATEADD(day, 1, evidence.evidence_date::DATE)) AS eligible_date,
        MAX(evidence.evidence_date) AS evidence_date
    FROM {{ ref('int_nice_immunosuppression_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate
        ON evidence.person_id = candidate.person_id
    GROUP BY evidence.person_id, evidence.evidence_type, eligible_date
),

selected_evidence AS (
    {% for evidence_type in ['IMMDX_COV_COD', 'IMMRX_COD', 'IMM_ADM_COD', 'DXT_CHEMO_COD'] %}
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        candidate.proxy_reference_date,
        evidence.evidence_type,
        evidence.evidence_date
    FROM candidates AS candidate
    ASOF JOIN (
        SELECT
            person_id,
            evidence_type,
            eligible_date,
            evidence_date
        FROM daily_evidence
        WHERE evidence_type = '{{ evidence_type }}'
    ) AS evidence
        MATCH_CONDITION (candidate.proxy_reference_date >= evidence.eligible_date)
        ON candidate.person_id = evidence.person_id
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

SELECT
    person_id,
    reporting_date,
    MAX(evidence_date) AS latest_evidence_date
FROM selected_evidence
WHERE evidence_type = 'IMMDX_COV_COD'
    OR (evidence_type IN ('IMMRX_COD', 'DXT_CHEMO_COD')
        AND evidence_date >= DATEADD(month, -6, proxy_reference_date))
    OR (evidence_type = 'IMM_ADM_COD'
        AND evidence_date >= DATEADD(year, -3, proxy_reference_date))
GROUP BY person_id, reporting_date
{% endmacro %}
