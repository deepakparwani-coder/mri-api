# Where the launch-pricing and collection defaults come from

You asked for industry practice rather than one person's view, so neither
default below is a judgement call. The escalation ladder is measured from your
own database; the collection profile is sourced and cited.

---

## 1. Escalation ladder — measured from LF data

**Method.** For every marketable wing in Whitefield and Hinjewadi at Q1 26-27,
compare `LAUNCH_PRICE_PSF` against `CURRENT_PRICE_PSF` and annualise over the
months since launch.

**Sample.** 355 wings across 118 projects, both cities, launched 0.5–15 years ago.

| Years since launch | n | Median uplift | Median CAGR | p25 | p75 |
|---|---|---|---|---|---|
| 0.5 – 2 | 200 | 1.01x | **1.1%** | 0.0% | 6.3% |
| 2 – 4 | 123 | 1.27x | **9.1%** | 4.2% | 11.0% |
| 7+ | 30 | 2.46x | 7.5% | 3.8% | 7.5% |

**Overall median 4.7%/yr.** Mean is 8.3%, pulled up by a long right tail — the
median is the honest central estimate.

### Three findings that changed the model

**Escalation is back-loaded, not linear.** Prices barely move for two years
after launch (1.1%/yr), then accelerate sharply (9.1%/yr). A straight-line
ladder mis-states the shape. The engine now uses a two-segment index.

**The realistic spread is much narrower than intuition suggests.** Applying the
measured index at phase midpoints and normalising to the target APR:

| Build | Data-derived factors | Launch | Final | Spread |
|---|---|---|---|---|
| 3-year | 0.978 / 0.989 / 1.039 | −2.2% | +3.9% | **1.06x** |
| 4-year | 0.952 / 0.962 / 1.011 | −4.8% | +10.3% | **1.16x** |

My earlier hand-picked ladder was 0.92 → 1.15, a **1.25x** spread — roughly
twice the escalation the market actually delivered on a 3-year build. Left in,
it would have overstated late-phase revenue on every report.

**Escalation is not guaranteed.** **15.8% of wings — about one in six — trade
BELOW their launch price today.** That is why the engine carries an explicit
downside case rather than treating the ladder as a certainty, and why the
output is instructed to present it as a strategy with a condition, never as
predicted appreciation.

Reproduce: the query is in `evidence_escalation.py`.

---

## 2. Collection profile — construction-linked plan

The previous assumption was 65% of a booking collected in-year with 35%
lagging. That is not how Indian residential is sold, and it flattered NPV
badly.

Under a **construction-linked plan (CLP)** the buyer pays a booking amount, then
instalments tied to construction milestones, then a final tranche at
possession/OC. Typical premium-project structure:

| Milestone | Share of price |
|---|---|
| Booking / agreement | 10–15% |
| Foundation / plinth | 10–15% |
| Slabs 1–2 | 10% |
| Each subsequent slab | 5–10% |
| Superstructure complete | 10% |
| Brickwork / plastering | 5–7% |
| Fit-outs / flooring | 5% |
| Possession / OC | 10–15% |

Source: [Piramal Realty — How CLP works](https://www.piramalrealty.com/blogs/construction-linked-payment-plan-how-clp-works),
corroborated by [Dwello](https://dwello.in/news/construction-linked-payment-plans-in-india-everything-you-need-to-know)
(booking ~10%, possession 5–10%).

**Statutory ceiling.** RERA **section 13(1)** prohibits a promoter from taking
more than **10% of the cost of the apartment** before a registered agreement for
sale. Any model assuming a larger up-front collection is not merely optimistic,
it is describing something unlawful.
Source: [Decoding RERA Section 13(1)](https://www.intolegalworld.com/post/decoding-rera-section-13-1-the-10-limit-on-advance-booking-amount),
[Section 13 bare text](https://ibclaw.in/section-13-of-real-estate-regulation-and-development-act-2016-rera-no-deposit-or-advance-to-be-taken-by-promoter-without-first-entering-into-agreement-for-sale/).

**Implemented as:** 12% on booking/agreement, 15% at possession, and the
remaining 73% accruing pro-rata to construction progress over the build period
still remaining for that cohort. Defaults are `booking_collect_pct` and
`possession_collect_pct`.

---

## 3. What correcting these two things did to the answer

| | Old assumptions | Evidence-based |
|---|---|---|
| Flat-price IRR | 44.4% | **19.9%** |
| Ladder at same duration | −₹4.89 Cr | −₹2.08 Cr |
| Ladder + compressed sell-out | +₹2.55 Cr | **+₹10.57 Cr** |

Two things worth noting.

**A 44% IRR was never credible** for Indian residential development. It was an
artefact of collecting 65% of every booking in-year. At ~20% the model is in the
range a land-acquisition committee would recognise.

**Your instinct gets stronger, not weaker — but for a specific reason.** Under a
real CLP, collections track construction progress, so compressing a four-year
build to three pulls the *entire* collection curve forward rather than just
booking amounts. Speed is worth far more than the old model implied: **+₹10.6 Cr
of NPV**, against a cost of ₹2.1 Cr if the discount fails to buy that speed.

So the recommendation stands, with the reasoning corrected. Launch discounting
pays for itself through **velocity**, not through appreciation. The escalation is
something the developer earns by selling fast enough to finish early — and in
this market one project in six never earned it.
