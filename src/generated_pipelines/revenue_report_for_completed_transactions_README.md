# Pipeline: `revenue_report_for_completed_transactions`

## Overview

| Property        | Value                                                                 |
|-----------------|-----------------------------------------------------------------------|
| **Pipeline**    | `revenue_report_for_completed_transactions`                           |
| **Owner**       | Data Engineering                                                      |
| **Runtime**     | PySpark 3.5+ / Delta Lake                                             |
| **Source**      | `s3://etl-agent-raw-prod/olist/orders/` (CSV)                         |
| **Target**      | `s3://etl-agent-processed-production/revenue_report_for_completed_transactions/` (Delta) |
| **Partitioning**| `order_purchase_year`, `order_purchase_month`                         |
| **Write Mode**  | `overwrite` (full refresh)                                            |

### Description

Filters the Olist orders dataset to include **only delivered orders** so that
downstream revenue reports reflect completed transactions only. The pipeline
renames raw generic columns to semantic names, casts date strings to proper
timestamps, removes incomplete records, and enriches the output with
year/month partition columns for efficient downstream querying.

---

## Architecture
