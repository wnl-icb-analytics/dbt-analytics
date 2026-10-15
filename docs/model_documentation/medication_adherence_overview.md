# Medicines Adherence (PDC) Overview

## Purpose

The pipeline computes rolling and overall Proportion of Days Covered (PDC) — the standard prescribing-based medication adherence measure — per person and drug (VTM grain) across five drug classes: RAAS, beta-blockers, calcium-channel blockers, non-insulin anti-diabetics and lipid lowering drugs (BNF paragraphs 020505, 020400, 020602, 060102, 021200).
This implementation is a faithful port of the AIC centre meds_adherence Snowpark pipeline deployed in NEL and SEL (`MedicationTableSnowpark`, `compute_pdc_rolling` with a 12-month rolling window, dynamic exposure denominator `pdc_type=2` and exclusive overlap counting) into WNL native dbt models. Every deliberate divergence from the original is documented in the model headers. Two column families are emitted: the faithful `pdc` / `overall_pdc` columns replicate the AIC measure exactly, while the `*_corrected` columns implement **PDC2** as defined by Prieto-Merino et al. (2021) — they fix the original's last-order quirk *and* right-censor all supply at the observation date, so days that cannot be observed enter neither numerator nor denominator. The observation date (`as_at_date`) is derived from the data, not the clock, and is emitted as a column so every row carries the cutoff it was computed under.

## Outstanding issues
- **Results not yet validated**: the logic is complete and builds, but `analyses/medication_adherence_pdc_validation.sql` has not been run through — sections 1–3 (VTM resolution coverage, reference ambiguity, chain fragmentation) are the decision gates.
- **Last-order quirk decision**: the original 0-fills `days_to_next_order` on the final order of each refill chain, so that order contributes ~0 covered days. Both faithful and corrected columns are emitted; which is authoritative for reporting is undecided pending validation.
- **No numeric parity check is possible**: tNo comparator data in WNL environment to validate against but have the ISPORE poster to replicate validation
- **VTM resolution reliability**: refill chains group on the VTM code resolved via `stg_reference_bnf_latest` (SNOMED tier, then 9-char BNF chemical-substance tier, then concept-display fallback). This does not have an automated refrash
- **Selection nuance**: the original filtered on its environment's `bnf_reference`; this port filters on the repo's canonical dm+d-derived `bnf_code` via `get_medication_orders`.
- **One upward bias is accepted, not corrected** (inherited from AIC — see *Preserved AIC artefacts* below): exposure overhanging the window frame counts, and overhang days are covered by construction. This pushes windowed PDC up, so absolute levels — particularly against the conventional 0.80 threshold — should be treated with caution until quantified. (The other inherited bias, unobserved future supply, is corrected in the `*_corrected` family.)
- **3-year processing horizon**: orders older than 3 years are excluded, so chains that began earlier are left-truncated and `overall_*` describes the horizon rather than full therapy history.
- Local unit testing of the dose-instruction parser (the frequency pattern precedence is replicated verbatim but only eyeball-validated via the analysis queries).

## Relevant Literature
- Prieto-Merino D, Mulick A, Armstrong C, Hoult H, Fawcett S, Eliasson L, et al. Estimating proportion of days covered (PDC) using real-world online medicine suppliers’ datasets. Journal of Pharmaceutical Policy and Practice. 2021;14.  [Open access URL](https://pubmed.ncbi.nlm.nih.gov/34965882/)
- Dalli LL, Kilkenny MF, Arnet I, Sanfilippo FM, Cummings DM, Kapral MK, et al. Towards better reporting of the proportion of days covered method in cardiovascular medication adherence: A scoping review and new tool TEN-SPIDERS. British Journal of Clinical Pharmacology. 2022;88(10):4427–42.  [Open access URL](https://pmc.ncbi.nlm.nih.gov/articles/PMC9546055/)
- PDC as the preferred adherence measure: Pharmacy Quality Alliance (PQA) adherence measure specifications [URL](https://www.pqaalliance.org/adherence-measures)
- Terminology and definitions for database adherence research: Raebel et al., (2013) *Standardizing terminology and definitions of medication adherence and persistence in research employing electronic databases*, Medical Care [Open access URL](https://pmc.ncbi.nlm.nih.gov/articles/PMC3727405/)

## Relationship to the AIC implementation

The measurement method is unchanged; the data plumbing and packaging are not. This section is the reference for anyone comparing WNL figures with the NEL/SEL deployments or the ISPOR poster.

### Identical to AIC (the method)
- The five drug classes and their exact labels.
- Oral-form restriction (exact match on `tablet`, `tablets`, `tab`, `capsule`, `capsules`, `cap`) and missing-value sentinel normalisation (`none`, `na`, `null`, `''`, `nan` → NULL).
- Dose-instruction parsing: all 29 frequency patterns **in their original precedence order**, the number-word/digit count extraction, `tablets_per_day = count × frequency`, and the edge cases that follow (a numeric match > 4 falls back to 1; an unmatched dose defaults to one per day).
- Duration derivation: `quantity ÷ tablets_per_day`, the null-unsafe `duration_flag`, fallback to recorded `duration_days`, and `order_enddate = order_date + calculated_duration`.
- Same-day de-duplication keeping the highest quantity, then `days_to_next_order` via `lead()` per (person, drug).
- Chain formation: one chain per (person, drug), with no gap-based splitting.
- Window generation: person-anchored monthly anchors from the chain's first order, each spanning 12 calendar months, count driven by `months_between(overall_end, overall_start)::int`.
- Order selection into a window (`order_date < window_end AND order_enddate_filled >= window_start`), the `pdc_type=2` dynamic exposure denominator, the `exclusive=True` early-refill subtraction, the `covered_days > 0` guard, uncapped PDC, empty windows retained with NULLs, and the chain-level overall PDC carried on every window row.

### Deliberately different

| Area | AIC | This implementation | Why |
|---|---|---|---|
| Source | `pipeline_prod.int_gp_medication_order` | OLIDS `stg_olids_medication_order` via `get_medication_orders` | Different environment; the macro also supplies person resolution and deleted-record filtering already standard in this repo |
| Class selection | `bnf_reference IN (…)` | `bnf_code` paragraph prefix via the macro | `bnf_code` is this repo's canonical dm+d-derived BNF column |
| Chain key | `vtm_concept_name` | VTM **code** via `stg_reference_bnf_latest` (SNOMED → 9-char BNF chemical substance → concept-display fallback); the VTM name is re-attached at the drill-down layer | OLIDS orders carry no VTM name; codes are stable keys, names are presentation |
| History processed | Full history | Last 3 years (`medication_adherence_order_lookback_years`) | Volume. Chains are left-truncated, so `overall_*` describes the horizon |
| Output shape | One table per drug class | One table with a `drug_class` column | Grouping keys already included the class; the loop only existed to name tables |
| Compute | Snowpark with `collect()` round-trips | Set-based SQL | No semantic effect |
| Quirk handling | Quirk only | Quirk (faithful) **plus** a `*_corrected` PDC2 family | See below |
| Observation cutoff | None — future supply counted | `as_at_date` applied to the `*_corrected` family | PDC2 bounds the denominator by the observation period |
| Repeat flag | None | `is_repeat_order` added | Used by the person-level mart's cohort; plays no part in the PDC |
| Person-level output | None | `fct_person_medication_adherence` (repeat + 1-year-recency cohort) | WNL addition, no AIC counterpart |

### Preserved AIC artefacts (deliberately not fixed in the faithful columns)

1. **Last-order quirk** — `days_to_next_order` is 0-filled on each chain's final order, so its entire duration is subtracted and it contributes ~0 covered days. **Fixed in the `*_corrected` family.**
2. **Future supply projected as covered** — supply intervals run past the observation date and count as covered, and exposure spans can end in the future. **Fixed in the `*_corrected` family** by right-censoring at `as_at_date`. Note the asymmetry this removes: because the denominator ends at last-supply exhaustion, unobserved days entered the calculation *only when covered* — a one-directional bias that neither PDC1 nor PDC2 has.
3. **Window overhang** — the window only *selects* orders; the exposure span (hence both numerator and denominator) may start before `window_start` and end after `window_end`, and overhang days are covered by construction, biasing boundary windows toward 1. **Not corrected in either family**: bounding exposure to the frame would make the measure a PDC1/PDC2 hybrid rather than PDC2 over the exposure span, and would put a third cause between the two families.
4. Uncapped PDC (values > 1 are possible), the null-unsafe `duration_flag`, and `months_between(…)::int` rounding.

Because censoring removes covered days, `pdc_corrected >= pdc` holds only where the exposure ends at or before `as_at_date`; the invariant tests exempt the rest. Censoring **truncates rather than drops** — an order whose supply straddles `as_at_date` contributes its elapsed portion to numerator and denominator alike, so a perfectly adherent person is unaffected while a person with gaps loses the inflation.

`as_at_date` = `least(max order date in the source, current_date)`, or the pinned value of the var `medication_adherence_as_at_date` (`YYYY-MM-DD`) for reproducing a historical run. Deriving it from the data rather than the clock means a rebuild on unchanged data reproduces the same numbers.

### Comparing output with an AIC deployment
Use the **faithful** columns and rebuild with a long horizon (`--vars '{medication_adherence_order_lookback_years: 20}'`). Residual differences should then be attributable to source coverage (OLIDS vs the AIC environment) and the VTM resolution route — data differences, not method differences.

## Model Flow
The pipeline uses OLIDS medication orders (via the `get_medication_orders` macro) and the BNF/dm+d reference (`stg_reference_bnf_latest`) for VTM resolution.

1. `int_medication_adherence_orders`
   - Selects orders in the five BNF paragraphs and labels `drug_class`, restricted to a 3-year processing horizon (month-truncated cutoff, `medication_adherence_order_lookback_years`).
   - Resolves `drug_name` to the VTM code (SNOMED tier → 9-char BNF chemical-substance tier → concept-display fallback, with a `drug_name_source` audit column).
   - Normalises missing-value sentinel strings to NULL, filters to oral solid forms (tablet/capsule quantity units), and flags repeat-authorised orders (`is_repeat_order`).

2. `int_medication_adherence_order_durations`
   - Parses free-text dose instructions to `tablets_per_day` (ordered regex precedence, replicated verbatim from the original — do not reorder).
   - Derives `calculated_duration` (quantity / tablets-per-day, falling back to recorded `duration_days`) and `order_enddate`.
   - Deduplicates same-day orders keeping the highest quantity; computes `days_to_next_order` per (person, drug) chain in faithful (0-filled) and corrected (NULL on last order) variants.

3. `int_medication_adherence_pdc`
   - Generates person-anchored monthly rolling 12-month windows from each chain's first order.
   - Computes windowed PDC with the dynamic exposure denominator and exclusive overlap subtraction, plus an overall chain PDC — a faithful AIC variant and a corrected PDC2 variant (quirk fixed, right-censored at `as_at_date`) with its own denominators.
   - Empty windows are retained with NULL exposure/PDC, matching the original.

4. `int_medication_adherence_pdc_windows`
   - Drill-down table (person × drug × window): joins `dim_person` and re-presents `drug_name` as the readable VTM name via a deduplicated 1:1 code→name map (the grain key remains `vtm_code`).

5. `fct_person_medication_adherence`
   - Person-grain mart for downstream use: per drug class, PDC aggregated across the person's qualifying chains — latest fully elapsed window and overall chain span.
   - Snapshot cohort: repeat-authorised chains with supply active in the last year (recency judged on supply end, not live-today repeat status, to avoid selecting the cohort on adherence itself). Class columns are NULL with `drug_count` 0 where there is no current repeat therapy.
   - No opt-out filter; secondary-use consumers join `DIM_PERSON_SECONDARY_USE_ALLOWED`.

Validation queries: `analyses/medication_adherence_pdc_validation.sql` (resolution coverage, reference ambiguity, fragmentation, funnel, dose-parse eyeball, PDC distributions, quirk sizing, single-person trace).
