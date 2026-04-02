# Pipeline: `revenue_report_for_completed_transactions`

## Overview

| Property        | Value                                                                                     |
|-----------------|-------------------------------------------------------------------------------------------|
| **Pipeline ID** | `revenue_report_for_completed_transactions`                                               |
| **Source**      | `s3://etl-agent-raw-prod/olist/orders/` (CSV)                                             |
| **Target**      | `s3://etl-agent-processed-production/revenue_report_for_completed_transactions/` (Delta)  |
| **Format**      | Delta Lake (Parquet-backed, partitioned)                                                  |
| **Write Mode**  | Overwrite (full refresh)                                                                  |
| **Partitions**  | `order_purchase_year`, `order_purchase_month`                                             |
| **PySpark**     | 3.5+                                                                                      |
| **Delta Lake**  | 3.x                                                                                       |

---

## Description

Filters the Olist e-commerce orders dataset to retain **only delivered orders**, ensuring
that downstream revenue reports reflect completed transactions only. The pipeline also
enriches the dataset with year/month partition columns derived from the purchase timestamp,
enabling efficient time-based partition pruning in analytical queries.

---

## Source Schema

The raw CSV files use generic column names (`col0`–`col7`). The pipeline renames them to
their semantic equivalents:

| Raw Column | Semantic Column                  | Type        |
|------------|----------------------------------|-------------|
| `col0`     | `order_id`                       | `string`    |
| `col1`     | `customer_id`                    | `string`    |
| `col2`     | `order_status`                   | `string`    |
| `col3`     | `order_purchase_timestamp`       | `timestamp` |
| `col4`     | `order_approved_at`              | `timestamp` |
| `col5`     | `order_delivered_carrier_date`   | `timestamp` |
| `col6`     | `order_delivered_customer_date`  | `timestamp` |
| `col7`     | `order_estimated_delivery_date`  | `timestamp` |

---

## Target Schema

| Column                           | Type        | Notes                              |
|----------------------------------|-------------|------------------------------------|
| `order_id`                       | `string`    | Non-null enforced                  |
| `customer_id`                    | `string`    |                                    |
| `order_status`                   | `string`    | Always `'delivered'` after filter  |
| `order_purchase_timestamp`       | `timestamp` |                                    |
| `order_approved_at`              | `timestamp` | May be null                        |
| `order_delivered_carrier_date`   | `timestamp` | May be null                        |
| `order_delivered_customer_date`  | `timestamp` | May be null                        |
| `order_estimated_delivery_date`  | `timestamp` | May be null                        |
| `order_purchase_year`            | `integer`   | Partition key (derived)            |
| `order_purchase_month`           | `integer`   | Partition key (derived)            |

---

## Transformation Steps
