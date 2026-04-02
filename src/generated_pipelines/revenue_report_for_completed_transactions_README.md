# Pipeline: `revenue_report_for_completed_transactions`

## Overview

| Property        | Value                                                                                     |
|-----------------|-------------------------------------------------------------------------------------------|
| **Pipeline**    | `revenue_report_for_completed_transactions`                                               |
| **Source**      | `s3://etl-agent-raw-prod/olist/orders/` (CSV)                                             |
| **Target**      | `s3://etl-agent-processed-production/revenue_report_for_completed_transactions/` (Delta)  |
| **Format**      | Delta Lake (overwrite, partitioned)                                                        |
| **Partitions**  | `order_purchase_year`, `order_purchase_month`                                             |
| **PySpark**     | 3.5+                                                                                      |
| **Delta Lake**  | 3.x                                                                                       |

---

## Description

Filters the Olist e-commerce orders dataset to retain **only delivered orders**,
removes rows with null `order_id` values, derives year/month partition columns from
`order_purchase_timestamp`, and writes the result as a partitioned Delta Lake table.

---

## Pipeline Steps
