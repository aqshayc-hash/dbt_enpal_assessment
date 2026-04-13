# dbt Sales Funnel Analytics — Enpal Assessment

A dbt project modelling a Pipedrive CRM sales funnel to produce a monthly reporting layer tracking deal progression across 9 funnel stages.

Built as part of a data engineering assessment. The original challenge brief is preserved at the bottom of this README.

---

## What I Built

### Problem
Raw Pipedrive CRM data needed to be transformed into a clean, analytics-ready reporting model tracking how deals move through a 9-stage sales funnel month by month — from initial lead generation through to renewal and expansion.

### Solution
A 3-layer dbt project (staging → intermediate → marts) on PostgreSQL, fully containerised with Docker Compose.

**Key output:** `rep_sales_funnel_monthly` — a mart table with columns `month`, `kpi_name`, `funnel_step`, `deals_count` enabling month-over-month funnel analysis and conversion rate tracking at every stage.

---

## Architecture

```
raw_data (PostgreSQL — Pipedrive CRM source tables)
    │
    ▼
staging/        ← light cleaning, renaming, type casting (views)
    │
    ▼
intermediate/   ← business logic, stage classification, deal joins (views)
    │
    ▼
marts/          ← rep_sales_funnel_monthly (table, query-ready)
```

### Funnel Stages Modelled

| Step | KPI |
|---|---|
| 1 | Lead Generation |
| 2 | Qualified Lead |
| 2.1 | Sales Call 1 |
| 3 | Needs Assessment |
| 3.1 | Sales Call 2 |
| 4 | Proposal / Quote Preparation |
| 5 | Negotiation |
| 6 | Closing |
| 7 | Implementation / Onboarding |
| 8 | Follow-up / Customer Success |
| 9 | Renewal / Expansion |

---

## Stack

| Tool | Purpose |
|---|---|
| dbt-core + dbt-postgres | Transformation layer |
| PostgreSQL (Docker) | Warehouse |
| Docker Compose | Local environment |

---

## Quick Start

**Prerequisites:** Docker Desktop, Python 3.8+

```bash
# 1. Start the database
docker compose up -d

# 2. Install dbt
pip install dbt-core dbt-postgres

# 3. Run the models
dbt deps
dbt run

# 4. Run tests
dbt test
```

Database credentials (local only):
```
Host: localhost  Port: 5432
User: admin      Password: admin  DB: postgres
```

Results land in the `public_pipedrive_analytics` schema. Query `rep_sales_funnel_monthly` for the final output.

---

## Key Design Decisions

- **Staging as views** — no storage cost for cleaning layers, always reflects source
- **Intermediate as views** — business logic stays reusable without materialisation overhead
- **Mart as table** — reporting layer is pre-aggregated for fast BI queries
- **Schema isolation** — all models land in `pipedrive_analytics` schema, separated from raw data

---

<details>
<summary>Original Challenge Brief</summary>

## Setup

1. Download Docker Desktop (if you don't have installed) using the official website, install and launch.
2. Fork this Github project to you Github account. Clone the forked repo to your device.
3. Open your Command Prompt or Terminal, navigate to that folder, and run the command `docker compose up`.
4. Now you have launched a local Postgres database with the following credentials:
```
Host: localhost  User: admin  Password: admin  Port: 5432
```
5. Connect to the db via a preferred tool (e.g. DataGrip, Dbeaver etc)
6. Install dbt-core and dbt-postgres using pip (if you don't have) on your preferred environment.
7. Now you can run `dbt run` with the test model and check public_pipedrive_analytics schema to see the dbt result (with one test model)

## Project
1. Remove the test model once you make sure it works
2. Dive deep into the Pipedrive CRM source data to gain a thorough understanding of all its details.
3. Define DBT sources and build the necessary layers organizing the data flow for optimal relevance and maintainability.
4. Build a reporting model (rep_sales_funnel_monthly) with monthly intervals, incorporating the funnel steps listed above.
5. Column names of the reporting model: `month`, `kpi_name`, `funnel_step`, `deals_count`
6. Git commit all the changes and create a PR to your forked repo.

</details>
