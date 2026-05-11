# Shopify → BigQuery ELT Pipeline

An automated Python ELT pipeline that extracts order and product sales data from the Shopify GraphQL API and loads it into Google BigQuery. Scheduled via GitHub Actions.

---

## Overview

This pipeline covers the following data domains:

- **Orders** — full historical load, incremental updates (last 30 days), and backfill capability
- **Product sales** — line-item level product sales data
- **Collections** — collections and what products belong in them
- **Product information** — data on product and their attributes
- **Variant information** — data on variants and their attributes


Data is written to the `shopify` dataset in BigQuery. Orders updates use a MERGE pattern to keep records current without full reloads.

---

## Project Structure

```
.
├── .github/
│   └── workflows/
│       ├── orders_pipeline.yml       # Scheduled: incremental orders update
│       └── products_orders_pipeline.yml       # Scheduled: product sales (line items) load
│       └── collections_pipeline.yml       # Scheduled: collections and products in those collections load
│       └── products_variants_pipeline.yml       # Scheduled: product and variants information load
│
├── src/
│   ├── utils.py                    # Shopify auth, API calls, bulk query polling
│   ├── df_functions.py             # Raw data → pandas DataFrames
│   ├── gcloud.py                   # BigQuery table schemas and write jobs
│   ├── queries_orders.py           # GraphQL queries (orders) + BigQuery SQL for MERGE
│   └── queries_product_orders.py   # GraphQL queries (product line item sales)
│   └── queries_products_variants.py  # GraphQL queries (product line item sales)
│   └── queries_collections.py          # GraphQL queries (product line item sales)
│
├── orders.py                       # Full historical orders load
├── orders_update.py                # Incremental update: last 30 days, MERGE into BQ
├── products_sales.py               # Product sales load
├── backfill_orders.py              # Backfill: overwrites orders table in full
├── collections_shopify.py          # Collections load
├── products_information.py         # Product information/attributes load (overwrites)
├── variants_information.py         # Variant information/attributes load (overwrites)
├── requirements.txt
└── README.md
```

---

## BigQuery Destinations

| Script | Target Table | Load Strategy |
|---|---|---|
| `orders.py` | `shopify.orders` | Append / full load |
| `orders_update.py` | `shopify.orders_update` | MERGE (upsert on order ID) |
| `products_sales.py` | `shopify.products_sales` | Append / full load |
| `backfill_orders.py` | `shopify.orders` | Full overwrite |
| `collections_shopify.py` | `shopify.collections_shopify` | Full overwrite |
| `products_information.py` | `shopify.products_information` | Full overwrite |
| `variants_information.py` | `shopify.variants_information` | Full overwrite |

---

## Prerequisites

- Python 3.9+
- A Google Cloud project with BigQuery enabled
- A Shopify private app with the appropriate API scopes (`read_orders`, `read_products`)
- A GCP service account with `BigQuery Data Editor` and `BigQuery Job User` roles

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/leobeta92/shopify-bigquery-pipeline.git
cd shopify-bigquery-pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file in the project root (never commit this):

```env
SHOPIFY_CLIENT_ID=your_shopify_client_id
SHOPIFY_SECRET=your_shopify_secret
SHOPIFY_STORE_URL=your-store.myshopify.com
```

> For local runs, also place your GCP service account key at the project root as `cloud_python_private_key.json` (see GCP credentials below).

### 4. GCP credentials

Authentication is handled automatically in code depending on the execution environment:

- **Locally** — place your GCP service account key file at the project root named `cloud_python_private_key.json`. The pipeline detects the absence of `GCP_SERVICE_ACCOUNT_KEY` and falls back to this file.
- **GitHub Actions** — the full service account JSON is read from the `GCP_SERVICE_ACCOUNT_KEY` secret and parsed at runtime. No key file is needed in the repo.

> Never commit `cloud_python_private_key.json`. Add it to `.gitignore`.

---

## Running Locally

```bash
# Full historical orders load
python orders.py

# Incremental update: fetch last 30 days and MERGE into BigQuery
python orders_update.py

# Product sales load
python products_sales.py

# Backfill: overwrite orders table (use with caution)
python backfill_orders.py

# Information about collections and what products belong in those collections.
python collections_shopify.py

# Information on products load
python products_information.py

# Information on variants load
python variants_information.py

```

---

## GitHub Actions (Scheduled Runs)

Two workflows run on cron schedules:

| Workflow | Script | Purpose |
|---|---|---|
| `orders_pipeline.yml` | `orders.py` `orders_update.py` | Keeps orders current — captures status changes, refunds, cancellations |
| `products_orders_pipeline.yml` | `products_sales.py` | Loads product line-item sales data |
| `products_variants_pipeline.yml` | `products_information.py` `variants_information.py` | Loads product and variant information data |
| `collections_pipeline.yml` | `collections_shopify.py` | Loads collections data |

### Secrets required in GitHub

Add the following to your repository's **Settings → Secrets and variables → Actions**.

**Shared across all workflows:**

| Secret | Description |
|---|---|
| `SHOPIFY_CLIENT_ID` | Shopify private app client ID |
| `SHOPIFY_SECRET` | Shopify private app secret |
| `GCP_SERVICE_ACCOUNT_KEY` | GCP service account JSON key (full JSON string, not base64) |

**Scoped per workflow** — set separately under `ORDERS`, `ORDERS_UPDATE`, and `PRODUCTS`:

| Secret | Description |
|---|---|
| `GCP_PROJECT_ID` | Google Cloud project ID for the target environment |
| `BQ_DATASET` | BigQuery dataset name (e.g. `shopify`) |

**Scoped to `MERCHANT`:**

| Secret | Description |
|---|---|
| `SHOPIFY_STORE_URL` | Your store URL (e.g. `your-store.myshopify.com`) |

---

## Update Strategy

`orders_update.py` implements an **upsert pattern** to handle order mutations (status changes, refunds, partial fulfillments) without full reloads:

1. Fetches all orders modified in the last 30 days via the Shopify GraphQL Bulk Operations API
2. Loads results to a staging table in BigQuery
3. Runs a `MERGE` statement (defined in `src/queries.py`) to update changed records and insert new ones in `shopify.orders_update`

This keeps the pipeline idempotent — safe to re-run without duplicating records.

---

## Dependencies

| Package | Purpose |
|---|---|
| `google-cloud-bigquery[pandas]` | BigQuery client with pandas integration |
| `pandas` | Data transformation |
| `pyarrow` | Parquet/Arrow serialization for BQ loads |
| `db-dtypes` | BigQuery-compatible pandas dtypes |
| `python-dotenv` | `.env` file loading |

---

## Notes

- `backfill_orders.py` **overwrites** the `shopify.orders` table. Run only when a full historical reload is required.
- Shopify's [Bulk Operations API](https://shopify.dev/docs/api/usage/bulk-operations/queries) is used for large data exports; `src/utils.py` handles polling until the bulk job completes.
- Table schemas are defined centrally in `src/gcloud.py` to keep BigQuery writes consistent across scripts.
