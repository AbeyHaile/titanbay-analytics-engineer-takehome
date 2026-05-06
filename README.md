
# 📊 Investor Support & Close Pressure Analysis

## 1. Business Problem

Investor Services (IS) needs to understand:

* **Who is driving support demand?**
* **When does support pressure occur?**
* **Where should support resources be prioritised?**

The challenge is that:

* Support tickets come from **multiple requester types** (investors, relationship managers)
* There is **no reliable shared identifier** between systems
* Support demand is likely **event-driven** (e.g. around fund closes), not uniform

---

## 2. What this model enables

An analyst can now:

* Identify **high-touch investors** driving support demand
* Detect **support pressure around fund “installment” events (closes)**
* Prioritise **high-value, high-risk fund events**
* Quantify **how concentrated support demand is**

---

## 3. How to navigate

- `models/staging` → cleaned source data  
- `models/intermediate` → requester resolution logic  
- `models/marts` → final analytical models  

Key models:
- `fact_support_tickets`
- `fact_investor_support_profile`
- `fact_partner_close_pressure` > Note: This model includes only tickets raised directly by investors, not relationship managers, to avoid incorrect attribution.

### Sources used

* `platform_investors` → investor base
* `platform_entities` → investor → partner mapping
* `platform_partners` → partner metadata
* `platform_fund_closes` → fund “installment” events
* `freshdesk_tickets` → support activity

### Key data issues

* No shared ID between tickets and platform users
* Matching relies on:

  * `requester_email`
* `partner_label` is inconsistent (~44% null)

### Decision

> I use **email matching as the primary linkage** and ignore `partner_label` due to inconsistency.

Fallback:

* investor email → investor
* RM email → relationship manager
* else → `unknown`

---

## 4. Entity Resolution

Tickets can be raised by:

| Type                 | Handling                      |
| -------------------- | ----------------------------- |
| Investor             | Directly mapped via email     |
| Relationship Manager | Mapped via RM email           |
| Unknown              | Retained but not force-mapped |

> I do not attempt to force attribution where linkage is uncertain to avoid incorrect joins.

---

## 5. Modelling Approach

Structured in layers:

```text
staging → intermediate → marts
```

### Staging

* Clean + standardise fields (emails, timestamps)

### Intermediate

* `int_ticket_requester_resolution`

  * Resolves ticket → investor / RM / unknown
  * Adds `resolved_partner_id`

### Marts

#### i. `fact_support_tickets`

* Grain: **1 row per ticket**
* Adds:

  * requester type
  * resolution flags
  * time-to-resolution

#### ii. `fact_investor_support_profile`

* Grain: **1 row per investor**
* Aggregates:

  * ticket volume
  * urgency
  * resolution time
* Outputs behavioural segments:

  * support tier
  * frustration status
  * engagement status

#### iii. `fact_partner_close_pressure`

* Grain: **partner_id + close_id**
* Links tickets to fund closes using a time window
* Measures:

  * tickets in final week before close
  * urgency
* Outputs:

  * pressure signals
  * high-value + high-risk flags

---

## 6. Grain Management

All models have explicit grains:

| Model                         | Grain                 |
| ----------------------------- | --------------------- |
| fact_support_tickets          | ticket_id             |
| fact_investor_support_profile | investor_id           |
| fact_partner_close_pressure   | partner_id + close_id |

To avoid row multiplication:

* Ticket → close joins are restricted using:

```sql
DATEDIFF(...) BETWEEN -7 AND 14
```

> This ensures tickets are only linked to relevant close windows.

---

## 7. Data Quality Decisions

* Do not use `partner_label` (inconsistent)
* Do not force unmatched tickets into investors
* Retain `unknown` requester type
* Accept that some tickets cannot be attributed

---

## 8. Key Insights

#### i. Support demand is highly concentrated

* **3.2% of investors generate 52.8% of tickets**

> A small group drives the majority of workload.

---

#### ii. Most fund events are operationally quiet

* Only **~27% of fund closes** generate any tickets in the final week

> Support pressure is event-specific, not constant.

---

#### iii. AUM does not predict support pressure

* High-value closes often have **zero tickets**
* Lower-value closes can generate activity

> Operational complexity matters more than capital size.

---

#### iv. Critical events are rare

* Only **7 closes (~4.5%)** are both:

  * high value (top 25% AUM)
  * high pressure

> These are the key events IS should prioritise.

---

## 9. Assumptions

* Email is a reliable proxy for identity
* Tickets represent meaningful support demand
* Tickets near close dates are operationally relevant
* RM-raised tickets are not attributed to specific investors unless clearly linked

---

## 10. Incremental & Performance

* `fact_support_tickets` is incremental:

  * `ticket_id` as unique key
  * 3-day lookback on `created_at`
* Windowed joins reduce unnecessary computation

---

## 11. Tests

* Unique + not null on primary keys
* Accepted values on:

  * requester_type
  * priority
  * status
* Logical test:

  * high-risk-high-value ⊆ high AUM

---

## 12. Short-term vs Long-term Improvements

### Short-term (this model)

* Resolve identity via email
* Apply conservative joins
* Accept partial attribution

### Long-term (ideal)

* Introduce **shared user ID across systems**
* Enforce structured ticket metadata:

  * investor_id
  * partner_id
* Remove reliance on manual fields (`partner_label`)

---

## 13. What I would build next

* RM-level performance model
* Ticket categorisation (NLP or tag standardisation)
* Forecasting model:

  * predict ticket spikes around closes
* SLA tracking:

  * resolution time breaches

---

## 14. How I worked with AI tools

I used AI tools (e.g. ChatGPT, Gemini, NotebookLM, Cursor) as a lightweight support tool during the task, mainly to:

- Sanity check modelling decisions (e.g. grain, joins, incremental logic)
- Improve clarity and structure of the README
- Generate and refine YAML documentation and tests

All SQL, modelling decisions, and insights were validated against the data and adjusted based on actual outputs.

## 15. Example Analysis

> Example queries demonstrating how the models can be used to generate insights (including investor concentration and close pressure patterns) are available here:

👉 [analyses/key_insights.sql](analyses/key_insights.sql)

## 16. Summary

This model transforms fragmented support and platform data into:

* A **clear view of who drives support demand**
* A **time-aware view of when pressure occurs**
* A **prioritisation framework for Investor Services**

> The key finding is that support demand is highly concentrated and event-driven, enabling targeted operational interventions rather than broad scaling.

