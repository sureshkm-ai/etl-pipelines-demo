# RFM Customer Segmentation Pipeline

## Overview

| Property    | Value                                                              |
|-------------|--------------------------------------------------------------------|
| Pipeline    | `rfm_customer_segmentation_pipeline`                               |
| Source      | `s3://etl-agent-raw-prod/amazon/orders/` (Parquet)                 |
| Target      | `s3://etl-agent-artifacts-prod/analytics/geo_revenue/` (Delta)     |
| Write Mode  | `overwrite` (schema evolution enabled)                             |
| PySpark     | 3.5+                                                               |
| Delta Lake  | ✅                                                                  |

Reads raw Amazon order transactions from S3, computes **Recency**, **Frequency**,
and **Monetary** (RFM) scores per customer using quintile ranking, assigns a
human-readable segment label, and persists the result as a Delta Lake table.

---

## Pipeline Architecture
