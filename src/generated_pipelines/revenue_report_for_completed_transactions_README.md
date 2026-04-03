# Pipeline: `revenue_report_for_completed_transactions`

## Overview

| Property        | Value                                                                                   |
|-----------------|-----------------------------------------------------------------------------------------|
| **Pipeline**    | `revenue_report_for_completed_transactions`                                             |
| **Description** | Filters the Olist orders dataset to include only delivered orders so that downstream revenue reports reflect completed transactions only. |
| **Source**      | `s3://etl-agent-raw-prod/olist/orders/` (CSV)                                           |
| **Target**      | `s3://etl-agent-processed-production/revenue_report_for_completed_transactions/` (Delta)|
| **Partitioning**| `order_purchase_year`, `order_purchase_month`                                           |
| **Write Mode**  | Delta Lake `overwrite` (schema overwrite enabled)                                       |
| **PySpark**     | 3.5+                                                                                    |
| **Delta Lake**  | ✅                                                                                      |

---

## Architecture
