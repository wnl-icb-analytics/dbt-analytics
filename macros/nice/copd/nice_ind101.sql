{% macro nice_ind101(reference='current') %}
{#-
    Calculate NICE IND101 for COPD members with MRC grade 3+ at each reference date.
    Args: reference is current or by_month.
    Returns: the IND101 detail columns, one person per reporting_date.
-#}
-- NICE IND101: https://www.nice.org.uk/indicators/ind101
WITH population AS (
    SELECT p.*, r.earliest_diagnosis_date::DATE AS diagnosis_date
    FROM ({{ nice_reference_population(reference) }}) p
    INNER JOIN ({{ nice_register('COPD', reference) }}) r
        ON p.person_id = r.person_id AND p.reporting_date = r.reporting_date
), mrc AS (
    -- Any qualifying score counts. The first in the window anchors subsequent offers.
    SELECT p.person_id, p.reporting_date, MIN(m.clinical_effective_date::DATE) AS mrc_date
    FROM population p
    INNER JOIN {{ ref('int_mrc_dyspnoea_all') }} m
        ON p.person_id = m.person_id AND m.mrc_grade >= 3
        AND m.clinical_effective_date::DATE > DATEADD(month, -15, p.reporting_date)
        AND m.clinical_effective_date::DATE <= p.reporting_date
    GROUP BY p.person_id, p.reporting_date
), candidates AS (
    SELECT p.*, m.mrc_date
    FROM population p INNER JOIN mrc m
        ON p.person_id = m.person_id AND p.reporting_date = m.reporting_date
), rehabilitation AS (
    SELECT person_id, clinical_effective_date::DATE AS event_date,
        CASE pr_obs_type
            WHEN 'Pulmonary Rehab Attended' THEN 'ATTENDED'
            WHEN 'Pulmonary Rehab Unsuitable' THEN 'UNSUITABLE'
            WHEN 'Pulmonary Rehab Offered' THEN 'OFFERED'
            WHEN 'Pulmonary Rehab Declined' THEN 'OFFERED'
        END AS event_type
    FROM {{ ref('int_referral_pulmonary_rehab') }}
    WHERE pr_obs_type IN ('Pulmonary Rehab Attended', 'Pulmonary Rehab Unsuitable',
        'Pulmonary Rehab Offered', 'Pulmonary Rehab Declined')
    UNION ALL
    SELECT person_id, event_date, 'OFFERED'
    FROM {{ ref('int_nice_copd_observations_all') }}
    WHERE evidence_type IN ('PULREHAB_OFFERED_COD', 'PULRHBOFF_COD')
), rehab_evidence AS (
    SELECT p.person_id, p.reporting_date,
        MAX(IFF(e.event_type = 'OFFERED' AND e.event_date > p.mrc_date, e.event_date, NULL))
            AS latest_offer_date,
        MAX(IFF(e.event_type = 'ATTENDED' AND e.event_date >= p.diagnosis_date
            AND e.event_date < p.mrc_date, e.event_date, NULL)) AS prior_attendance_date,
        MAX(IFF(e.event_type = 'UNSUITABLE', e.event_date, NULL)) AS latest_unsuitable_date
    FROM candidates p
    LEFT JOIN rehabilitation e ON p.person_id = e.person_id AND e.event_date <= p.reporting_date
    GROUP BY p.person_id, p.reporting_date
), invitations AS (
    SELECT p.person_id, p.reporting_date, MIN(e.event_date) AS first_invitation_date,
        MAX(e.event_date) AS last_invitation_date
    FROM candidates p
    INNER JOIN {{ ref('int_nice_copd_observations_all') }} e
        ON p.person_id = e.person_id AND e.evidence_type = 'COPDINVITE_COD'
        AND e.event_date > DATEADD(month, -15, p.reporting_date)
        AND e.event_date <= p.reporting_date
    GROUP BY p.person_id, p.reporting_date
), responses AS (
    SELECT i.person_id, i.reporting_date, MAX(e.clinical_effective_date::DATE) AS latest_response_date
    FROM invitations i
    INNER JOIN {{ ref('int_ltc_review_all') }} e
        ON i.person_id = e.person_id AND e.review_type = 'COPD_REVIEW'
        AND e.clinical_effective_date::DATE > i.first_invitation_date
        AND e.clinical_effective_date::DATE <= i.reporting_date
    GROUP BY i.person_id, i.reporting_date
), assessed AS (
    SELECT p.*, e.latest_offer_date, e.prior_attendance_date, e.latest_unsuitable_date,
        i.first_invitation_date, i.last_invitation_date, r.latest_response_date,
        COALESCE(DATEDIFF(day, i.first_invitation_date, i.last_invitation_date) >= 7
            AND r.latest_response_date IS NULL, FALSE) AS is_excluded_invitation_non_response
    FROM candidates p
    LEFT JOIN rehab_evidence e ON p.person_id = e.person_id AND p.reporting_date = e.reporting_date
    LEFT JOIN invitations i ON p.person_id = i.person_id AND p.reporting_date = i.reporting_date
    LEFT JOIN responses r ON p.person_id = r.person_id AND p.reporting_date = r.reporting_date
)
SELECT person_id, 'IND101' AS indicator_id, 'COPD: offered pulmonary rehabilitation' AS indicator_name,
'The percentage of patients with COPD and Medical Research Council (MRC) Dyspnoea Scale of 3 or more at any time in the preceding 15 months, with a subsequent record of an offer of referral to a pulmonary rehabilitation programme.' AS indicator_description,
    reporting_date, DATEADD(month, -15, reporting_date) AS measurement_period_start,
    age, 'COPD with breathlessness grade 3 or more' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    diagnosis_date, mrc_date, latest_offer_date,
    first_invitation_date, last_invitation_date, latest_response_date, is_excluded_invitation_non_response,
    latest_offer_date AS latest_record_date, TRUE AS is_in_denominator,
    latest_offer_date IS NOT NULL AS is_in_numerator,
    IFF(latest_offer_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
WHERE prior_attendance_date IS NULL AND latest_unsuitable_date IS NULL
    AND NOT is_excluded_invitation_non_response
{% endmacro %}
