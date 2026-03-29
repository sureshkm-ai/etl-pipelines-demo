# rfm_customer_segmentation_pipeline

## Overview

| Property    | Value                                                              |
|-------------|--------------------------------------------------------------------|
| Pipeline    | `rfm_customer_segmentation_pipeline`                               |
| Source      | `s3://etl-agent-raw-prod/amazon/orders/` (Parquet)                 |
| Target      | `s3://etl-agent-artifacts-prod/analytics/geo_revenue/` (Delta)     |
| Operations  | `filter` → `aggregate` → `enrich`                                  |
| Write mode  | `overwrite` (schema evolution enabled)                             |

Reads raw Amazon order transactions from S3, computes **Recency**, **Frequency**,
and **Monetary** (RFM) metrics per customer, assigns quintile-based scores, and
writes the segmented customer table to a Delta Lake target.

---

## Pipeline Architecture
