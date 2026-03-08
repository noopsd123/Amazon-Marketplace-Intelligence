# Amazon Marketplace Intelligence
> A full-stack data engineering and business intelligence project that tracks, transforms, and visualizes Amazon product pricing, quality, and market dynamics — built on BigQuery, dbt, and Power BI.

![Pipeline](https://img.shields.io/badge/Pipeline-Prefect-blue) ![Transform](https://img.shields.io/badge/Transform-dbt-orange) ![Warehouse](https://img.shields.io/badge/Warehouse-BigQuery-yellow) ![BI](https://img.shields.io/badge/BI-PowerBI-gold) ![Status](https://img.shields.io/badge/Status-Active-brightgreen)

---

## 📌 Overview

Amazon sellers and product analysts face three problems every day: they don't know where prices are heading, which categories have unmet demand, and which products are actually worth competing against. Spreadsheets can't answer these questions at scale across 26,000 products.

The project answers real business questions: *Which categories have supply gaps? Where are the best-value products? How do prices shift week over week?* It is designed for product managers, brand analysts, and marketplace sellers who need data-driven answers — not guesswork.
This project builds the infrastructure and analytics layer that makes those answers instant.

A fully automated ELT pipeline extracts Amazon product data, loads it into BigQuery, and transforms it through dbt into clean, tested analytical models. A 4-page Power BI dashboard sits on top — built for four different audiences: executives tracking market health, analysts studying competitive positioning, product teams identifying launch opportunities, and sales teams monitoring price movements.

 
**Stack:** Python · Google BigQuery · dbt · SQL · Power BI · Prefect · GitHub Actions

---
## 📦 Dataset

**Source:** [Octaprice Ecommerce Product Dataset]([https://www.kaggle.com/](https://github.com/octaprice/ecommerce-product-dataset/tree/main)) — a real-world Amazon 
product catalog snapshot capturing pricing, ratings, reviews, and availability across hundreds of categories.

| Attribute | Detail |
|---|---|
| **Total Records** | ~26,000 product snapshots |
| **Categories** | 20+ top-level Amazon categories |
| **Price Range** | $0.19 — $2,399.99 USD |
| **Rating Range** | 0.0 — 5.0 stars |
| **Key Fields** | Product ID, Sale Price, Listed Price, Rating, Review Count, In-Stock Status, Category, Brand |
| **Granularity** | One row per product per snapshot date |
| **Format** | CSV → loaded into Google BigQuery |



## ✨ Key Features

- **Automated ELT Pipeline** — Python extracts raw data, loads into BigQuery, dbt transforms into analytics-ready mart tables
- **Weekly Snapshot Tracking** — Products tracked over time with week-over-week price, rating, and review change calculations
- **4-Page Interactive Dashboard** — Market Health, Competitive Analysis, Product Analytics, and Launch Readiness
- **Launch Readiness Scoring** — Identifies products meeting all 3 criteria: Rating ≥ 4.0, In Stock, 500+ Reviews
- **Value Score Algorithm** — Composite metric combining rating and discount depth to surface best-value products
- **Supply Gap Detection** — Flags categories where in-stock rate falls below 85% threshold
- **Data Quality Guards** — dbt tests enforce non-null constraints and accepted ranges on all key metrics

---

## 📁 File Structure

```
amazon-price-analytics/
│
├── extract/                        # Data extraction scripts
│   └── extract.py                  # Pulls raw data from source
│
├── load/                           # BigQuery loading scripts
│   └── load.py                     # Loads CSVs into raw BigQuery tables
│
├── transform/                      # dbt transformation layer
│   └── ecommerce_dbt/
│       ├── dbt_project.yml         # dbt project configuration
│       ├── profiles.yml            # BigQuery connection profile
│       ├── models/
│       │   ├── staging/            # Raw → cleaned staging models
│       │   │   └── stg_product_snapshots.sql
│       │   └── marts/              # Business-ready analytical models
│       │       ├── fct_weekly_snapshots.sql   # Core fact table
│       │       ├── dim_products.sql           # Product dimension
│       │       ├── dim_categories.sql         # Category dimension
│       │       └── schema.yml                 # dbt tests & documentation
│       ├── analyses/               # Ad-hoc analytical queries
│       ├── macros/                 # Reusable SQL macros
│       ├── seeds/                  # Static reference data (CSV)
│       ├── snapshots/              # dbt snapshot configs
│       └── tests/                  # Custom dbt data tests
│
├── .github/                        # GitHub Actions CI/CD workflows
├── .env.example                    # Environment variable template
├── .gitignore                      # Git ignore rules
├── .sqlfluff                       # SQL linting configuration
├── prefect.yaml                    # Prefect orchestration config
├── requirements.txt                # Python dependencies
├── start.ps1                       # Windows quickstart script
└── README.md                       # This file
```

---

## ⚙️ Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE FLOW                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  📦 Source Data          🐍 Extract & Load                 │
│  Octaprice Dataset  ───▶  Python Scripts  ───▶  BigQuery   │
│  (CSV files)              extract.py              raw_data  │
│                           load.py                 dataset   │
│                                                             │
│  🔄 Transform                    📊 Visualize              │
│  dbt Models          ───────▶   Power BI Desktop            |
│  staging/                        3-page Dashboard           │
│  └─ stg_product_snapshots        Page 1: Market Health      │
│  marts/                          Page 2: Competitive        │
│  ├─ fct_weekly_snapshots         Page 3: Launch Readiness   │
│  ├─ dim_products                                            │
│  └─ dim_categories                                          │
│                                                             │
│  🔁 Orchestration               ✅ Quality Gates           │
│  Prefect (prefect.yaml)          dbt tests (schema.yml)     │
│  Scheduled weekly runs           not_null, accepted_range   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
<img width="2512" height="1664" alt="Gemini_Generated_Image_a5qcu2a5qcu2a5qc" src="https://github.com/user-attachments/assets/894d0dab-2374-4d5a-bf76-02ddb96f65f7" />

```

### Pipeline Stages

**1. Extract** — `extract/extract.py`
Downloads raw product snapshot data from the source. Handles pagination, retries, and raw file storage.

**2. Load** — `load/load.py`  
Loads raw CSV files into BigQuery `raw_data` dataset. Schema is inferred automatically. Append-only strategy preserves history.

**3. Transform** — `transform/ecommerce_dbt/`  
dbt runs two layers:
- `staging/` — Cleans column names, casts types, removes nulls
- `marts/` — Calculates business metrics: discount %, WoW changes, 4-week volatility, snapshot deduplication via `QUALIFY ROW_NUMBER()`

**4. Visualize** — Power BI Desktop  
Connects directly to BigQuery `dbt_dev` dataset. All metrics calculated as DAX measures on top of the mart tables.

---

## 🔧 Configuration

### Environment Variables (`.env`)

| Variable | Description | Example |
|---|---|---|
| `GCP_PROJECT_ID` | Google Cloud project ID | `ecommerce-price-analytics` |
| `BIGQUERY_DATASET` | Target BigQuery dataset | `dbt_dev` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service account JSON | `./keys/sa.json` |

### dbt Profile (`transform/ecommerce_dbt/profiles.yml`)

```yaml
ecommerce_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: "{{ env_var('BIGQUERY_DATASET') }}"
      threads: 4
      timeout_seconds: 300
```

---

### Dashboard Pages — Stakeholders & Use Cases

#### Page 1 — Market Health
**Stakeholders:** Executive leadership, category managers, market analysts

| Business Question | Visual |
|---|---|
| How many products are we tracking and what is the avg quality? | KPI cards — total products, avg rating, avg price |
| Which categories dominate the market? | Category distribution chart |
| What is the overall discount landscape? | Avg discount by category |
| How do ratings distribute across the catalog? | Rating distribution histogram |

**Example scenario:** A category manager asks, *"Are our Electronics ratings above market average?"* → Filter to Electronics on Page 1 → compare Avg Rating card against the benchmark line.

---

#### Page 2 — Competitive Analysis
**Stakeholders:** Brand strategists, pricing teams, product marketers

| Business Question | Visual |
|---|---|
| Which products offer the best price-to-quality ratio? | Price vs Quality scatter (quadrant chart) |
| Which categories discount most aggressively? | Avg Discount Depth by Category bar |
| Which brands own the most review volume (search authority)? | Market Share by Review Volume |
| How does pricing architecture vary by category? | Price Architecture stacked bar |
| Which products have the highest composite value score? | Highest Value Products table |

**Example scenario:** A pricing strategist asks, *"Where are competitors discounting most to win market share?"* → Page 2 → Avg Discount Depth chart → filter to your category → benchmark your discount against the category average.

---

#### Page 3 — Product Analytics & Launch Readiness
**Stakeholders:** Product launch teams, marketplace sellers, e-commerce entrepreneurs

| Business Question | Visual |
|---|---|
| How many products meet all launch-readiness criteria? | Launch Ready Count & Rate KPI cards |
| Which categories have the most proven launch opportunities? | Launch-Ready Products by Category |
| Where are supply gaps that signal unmet demand? | Supply Availability Gaps (in-stock rate) |
| How crowded is each category? Where are niches? | Category Crowding donut chart |
| What does the competitive price landscape look like? | Price Range by Category table |
| Which specific products should I study or compete against? | Launch Candidates table |

**Launch criteria:** Rating ≥ 4.0 · In Stock = True · Reviews ≥ 500

**Example scenario:** A seller asks, *"I want to launch in a category with demand but low competition."* → Page 3 → filter Supply Availability below 85% → cross-reference with Category Crowding donut for small-tile categories → those are your target markets.

---

#### Page 4 — Sales Performance
**Stakeholders:** Sales operations, finance, revenue analysts

| Business Question | Visual |
|---|---|
| How have prices shifted week over week? | Price change WoW trend |
| Which products have the highest price volatility? | Price range 4-week rolling window |
| Where are new reviews being generated fastest? | Review velocity by category |

---

---

## 📬 Contact

**Noopur Divekar**  
Master's in Data Science — Indiana University Bloomington
Graduating in May 2026
📧 [LinkedIn](https://www.linkedin.com/in/noopurd)  
💻 [GitHub](https://github.com/noopsd123)

---

*Built with dbt + BigQuery · Dashboard by Noopur Divekar · Data: Octaprice Ecommerce Product Dataset*
