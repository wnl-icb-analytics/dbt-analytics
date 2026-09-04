/*
Person-level outpatient DNA rate from SUS, with empirical-Bayes shrinkage

Processing:
- Reduce appointments to one row per person over a rolling 12 months
- Fit a beta prior to the population, stratified by age band
- Return each person's observed rate, shrunk rate and how much evidence
  the shrunk rate rests on

Clinical Purpose:
- Identifying patients who repeatedly miss outpatient appointments
- Distinguishing a genuinely high DNA rate from one inferred off very few
  appointments, so outreach is not targeted on noise

Includes ALL persons (active, inactive, deceased) with at least one DNA
opportunity in the window, following intermediate layer principles.

Denominator
-----------
appointment_attended_or_dna codes:
  3  did not attend, no advance warning  -> numerator and denominator
  5  attended on time                    -> denominator
  6  arrived late, seen                  -> denominator
  2  cancelled by the patient            -> excluded, a different behaviour
  4  cancelled by the provider           -> excluded, not the patient's doing
  7  arrived late, not seen              -> excluded, ambiguous between
                                            patient and service failure and
                                            0.02% of activity
Provider cancellations are the largest excluded group. Counting all booked
appointments as the denominator gives a non-attendance rate near 31%; the
DNA rate on this denominator is 7.99%, in line with published NHS
outpatient figures.

Shrinkage
---------
A person's observed rate is unreliable when they have few appointments: a
third of this population has exactly one DNA opportunity, where the
observed rate can only be 0% or 100%. Each person's rate is therefore
shrunk towards the population rate for their age band, weighted by how
much evidence they have:

  shrunk rate = (dna + M * prior mean) / (opportunities + M)

M is the prior strength in pseudo-appointments, fitted from the data by the
ANOVA moment estimator of the intraclass correlation:

  p_bar = sum(y) / sum(n)
  MSB   = sum( n_i * (p_i - p_bar)^2 ) / (N - 1)
  MSW   = sum( y_i * (n_i - y_i) / n_i ) / (sum(n) - N)
  n_0   = ( sum(n) - sum(n^2)/sum(n) ) / (N - 1)
  rho   = (MSB - MSW) / (MSB + (n_0 - 1) * MSW)
  M     = (1 - rho) / rho

This estimator uses every person including those with a single appointment,
rather than fitting on frequent attenders only, which would bias the prior:
frequent attenders are behaviourally different from one-off attenders, and
one-off attenders are the group the prior actually governs.

Why the mean is stratified by age but the strength is not
---------------------------------------------------------
Band DNA rates range from 5.7% (75-84) to 12.6% (18-24), a 2.2-fold spread
that a single prior would wash out. Fitting M within band instead gives
3.20 to 4.08 against a pooled 3.59 - a spread small enough, and
non-monotone enough, that pooling the strength loses nothing and avoids
estimating a noisier parameter eight times.

Both an age-adjusted and an age-neutral shrunk rate are returned. Once age
enters the prior, a young person with no history scores higher than an
older person with no history on age alone. That is a legitimate
epidemiological signal and a fairness question, depending on the use, so
the model exposes both rather than deciding for its consumers. The
difference between the two columns is the age contribution.

Known limitations
-----------------
- People with exactly one opportunity DNA at 11.1% against 7.8% for
  everyone else, and this survives controlling for first-attendance mix.
  It may be a real behavioural difference or an artefact of a DNA ending
  the pathway while attendance generates follow-ups. The prior is not
  stratified on appointment count, because under the second explanation
  that would encode reverse causation. Scores for this group are therefore
  mildly conservative.
- The prior is refitted on every build, so a person's shrunk rate can move
  slightly without their own behaviour changing. op_dna_prior_mean and
  op_dna_prior_strength record the values used.
- SUS reporting delay means the most recent weeks are under-reported. This
  affects the level of every rate here.
*/

/* 1. FILTER TO PERIOD AND OUTCOMES OF INTEREST AND DERIVE RELEVANT TOTALS */
with appointments as ( 
    select
        sk_patient_id,
        visit_occurrence_id,
        appointment_attended_or_dna as dna_code,
        age_at_event,
        start_date
    from {{ ref('int_sus_op_appointment') }}
    where start_date between dateadd(month, -12, current_date()) and current_date()
      and sk_patient_id is not null
      and sk_patient_id != 1
),

-- One row per person. age_at_latest_appointment uses the person's most
-- recent appointment in the window; ties on start_date resolve arbitrarily,
-- which cannot move a person more than one age band.
person_appointments as (
    select
        sk_patient_id,
        count(distinct case when dna_code in ('3', '5', '6') then visit_occurrence_id end) as opportunities,
        count(distinct case when dna_code = '3' then visit_occurrence_id end) as dna,
        max_by(age_at_event, start_date) as age_at_latest_appointment
    from appointments
    group by sk_patient_id
),

-- SUS codes neonates as 7001-7007 (age in days), so out-of-range values are
-- expected. They join 'unknown' rather than being misbanded, and take the
-- population prior mean instead of a band mean.
person_scored as (
    select
        sk_patient_id,
        opportunities,
        dna,
        case
            when age_at_latest_appointment is null then 'unknown'
            when age_at_latest_appointment < 0 or age_at_latest_appointment > 120 then 'unknown'
            when age_at_latest_appointment < 18 then '0-17'
            when age_at_latest_appointment < 25 then '18-24'
            when age_at_latest_appointment < 35 then '25-34'
            when age_at_latest_appointment < 50 then '35-49'
            when age_at_latest_appointment < 65 then '50-64'
            when age_at_latest_appointment < 75 then '65-74'
            when age_at_latest_appointment < 85 then '75-84'
            else '85+'
        end as age_band
    from person_appointments
    where opportunities > 0
),

prior_mean_population as (
    select sum(dna) / sum(opportunities)::double as prior_mean
    from person_scored
),

prior_mean_by_age_band as (
    select
        age_band,
        sum(dna) / sum(opportunities)::double as prior_mean
    from person_scored
    where age_band != 'unknown'
    group by age_band
),

/* 2. CALCULATE WITHIN PERSON MEAN SQUARE AND BETWEEN PERSON MEAN SQUARE TO ESTIMATE CORRELATION BETWEEN APPOINMENT OUTCOMES FROM SAME PERSON */
-- The population mean is cross joined rather than subtracted afterwards so
-- the between-person sum of squares stays a sum of non-negative terms
-- instead of a difference between two large, similar quantities.
population_moments as (
    select
        count(*) as n_people,
        sum(p.opportunities) as n_total,
        sum(power(p.opportunities, 2)) as sum_n_squared,
        sum(p.opportunities * power(p.dna::double / p.opportunities - g.prior_mean, 2)) as sum_squares_between,
        sum(p.dna::double * (p.opportunities - p.dna) / p.opportunities) as sum_squares_within
    from person_scored as p
    cross join prior_mean_population as g
),

population_mean_squares as (
    select
        sum_squares_between / nullif(n_people - 1, 0) as mean_square_between,
        sum_squares_within / nullif(n_total - n_people, 0) as mean_square_within,
        (n_total - sum_n_squared / n_total::double) / nullif(n_people - 1, 0) as effective_opportunities
    from population_moments
),

population_correlation as (
    select
        (mean_square_between - mean_square_within)
            / nullif(mean_square_between + (effective_opportunities - 1) * mean_square_within, 0) as rho
    from population_mean_squares
),

/* 3. USE P TO DERIVE M AND SET STRENGTH OF PRIOR */
-- rho at or below zero means no detectable variation between people beyond
-- chance, whose correct limit is complete pooling (M tends to infinity).
-- The floor caps that at M of about 999 so the model still returns rows;
-- a value that large is a degenerate fit, which the prior strength test
-- catches rather than letting every score collapse to the prior silently.
prior_strength as (
    select
        (1 - rho_floored) / rho_floored as prior_strength
    from (
        select
            case
                when rho is null or rho <= 0.001 then 0.001
                else rho
            end as rho_floored
        from population_correlation
    )
),

person_prior as (
    select
        p.sk_patient_id,
        p.opportunities,
        p.dna,
        p.age_band,
        s.prior_strength,
        coalesce(b.prior_mean, g.prior_mean) as prior_mean_age_adjusted,
        g.prior_mean as prior_mean_age_neutral
    from person_scored as p
    cross join prior_strength as s
    cross join prior_mean_population as g
    left join prior_mean_by_age_band as b
        on p.age_band = b.age_band
),

/* 4. CALCULATE POSTERIOR */
-- Posterior parameters for the age-adjusted prior. Both posteriors share the
-- same total (opportunities + prior strength), so only alpha differs.
person_posterior as (
    select
        *,
        dna + prior_strength * prior_mean_age_adjusted as posterior_alpha,
        opportunities - dna + prior_strength * (1 - prior_mean_age_adjusted) as posterior_beta,
        opportunities + prior_strength as posterior_total
    from person_prior
)

select
    sk_patient_id

    /* Evidence */
    , opportunities as op_dna_opportunities_12mo
    , dna as op_dna_tot_12mo
    , round(dna::double / opportunities, 6) as op_dna_rate_12mo

    /* Shrunk estimates */
    , round(posterior_alpha / posterior_total, 6) as op_dna_rate_shrunk_12mo
    , round(
        (dna + prior_strength * prior_mean_age_neutral) / posterior_total, 6
      ) as op_dna_rate_shrunk_age_neutral_12mo
    , round(
        sqrt(posterior_alpha * posterior_beta
             / (power(posterior_total, 2) * (posterior_total + 1))), 6
      ) as op_dna_rate_posterior_sd_12mo
    , round(opportunities / posterior_total, 6) as op_dna_evidence_weight_12mo

    /* Prior actually applied, for audit */
    , age_band as op_dna_prior_age_band
    , round(prior_mean_age_adjusted, 6) as op_dna_prior_mean
    , round(prior_strength, 6) as op_dna_prior_strength

from person_posterior
