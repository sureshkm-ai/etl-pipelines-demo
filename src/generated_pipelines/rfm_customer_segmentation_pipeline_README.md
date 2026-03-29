# RFM Customer Segmentation Pipeline

## Overview

| Property    | Value                                                              |
|-------------|--------------------------------------------------------------------|
| Pipeline    | `rfm_customer_segmentation_pipeline`                               |
| Source      | `s3://etl-agent-raw-prod/amazon/orders/` (Parquet)                 |
| Target      | `s3://etl-agent-artifacts-prod/analytics/geo_revenue/` (Delta)     |
| Operations  | `filter` → `aggregate` → `enrich`                                  |
| Write mode  | Delta overwrite (schema evolution enabled)                         |

Reads raw Amazon order transactions from S3, computes per-customer **Recency**,
**Frequency**, and **Monetary** base metrics, assigns quintile-based RFM scores,
and writes the enriched result as a Delta Lake table for downstream analytics.

---

## Pipeline Architecture
