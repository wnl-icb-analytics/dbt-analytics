{#-
    Calculate NICE IND113 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND113 detail columns, one person per reporting_date.
-#}
{% macro nice_ind113(reference='current') %}
-- NICE IND113: https://www.nice.org.uk/indicators/ind113
-- Cancer care review within three months of a new diagnosis from 18 to 3 months before the reporting date, excluding qualifying invitation non-response.
WITH indicator_population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        profile.latest_cancer_diagnosis_date AS diagnosis_date,
        review.first_cancer_care_review_after_diagnosis_date AS latest_review_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS review
        ON population.person_id = review.person_id
        AND population.reporting_date = review.reporting_date
    WHERE profile.has_cancer
        AND profile.latest_cancer_diagnosis_date
            BETWEEN DATEADD(month, -18, population.reporting_date) AND DATEADD(month, -3, population.reporting_date)
),
invitation_dates AS (
    SELECT population.person_id, population.reporting_date,
        MIN(invitation.clinical_effective_date::DATE) AS first_invitation_date,
        MAX(invitation.clinical_effective_date::DATE) AS last_invitation_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_cancer_care_review_invitations_all') }} AS invitation
        ON population.person_id = invitation.person_id
        AND invitation.clinical_effective_date::DATE BETWEEN population.diagnosis_date
            AND LEAST(DATEADD(month, 3, population.diagnosis_date), population.reporting_date)
    GROUP BY population.person_id, population.reporting_date
),
responses AS (
    -- A later review can answer invitations even when the first review preceded them.
    SELECT invitation.person_id, invitation.reporting_date,
        MAX(review.clinical_effective_date::DATE) AS latest_response_date
    FROM invitation_dates AS invitation
    INNER JOIN {{ ref('int_ltc_review_all') }} AS review
        ON invitation.person_id = review.person_id
        AND review.review_type = 'CANCER_CARE_REVIEW'
        AND review.clinical_effective_date::DATE > invitation.first_invitation_date
        AND review.clinical_effective_date::DATE <= invitation.reporting_date
    GROUP BY invitation.person_id, invitation.reporting_date
),
assessed AS (
    SELECT population.*, invitation.first_invitation_date, invitation.last_invitation_date,
        COALESCE(DATEDIFF(day, invitation.first_invitation_date, invitation.last_invitation_date) >= 7
            AND response.latest_response_date IS NULL, FALSE) AS is_excluded_invitation_non_response,
        COALESCE(population.latest_review_date BETWEEN population.diagnosis_date
            AND LEAST(DATEADD(month, 3, population.diagnosis_date), population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    LEFT JOIN invitation_dates AS invitation
        ON population.person_id = invitation.person_id
        AND population.reporting_date = invitation.reporting_date
    LEFT JOIN responses AS response
        ON population.person_id = response.person_id
        AND population.reporting_date = response.reporting_date
)
SELECT person_id, 'IND113' AS indicator_id, 'Cancer: 3-month review' AS indicator_name,
'The percentage of patients with cancer diagnosed within the preceding 15 months who have a review recorded as occurring within 3 months of the practice receiving confirmation of the diagnosis.' AS indicator_description,
    reporting_date, DATEADD(month, -18, reporting_date) AS measurement_period_start,
    age, 'Cancer diagnosed 18 to 3 months ago'::VARCHAR(43) AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    diagnosis_date, latest_review_date, first_invitation_date, last_invitation_date,
    is_excluded_invitation_non_response,
    IFF(is_in_numerator, latest_review_date, NULL) AS latest_record_date,
    TRUE AS is_in_denominator, is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
WHERE NOT is_excluded_invitation_non_response
{% endmacro %}
