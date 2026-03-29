# RFM Customer Segmentation Pipeline

## Overview

| Property    | Value                                                        |
|-------------|--------------------------------------------------------------|
| Pipeline    | `rfm_customer_segmentation_pipeline`                         |
| Source      | `s3://etl-agent-raw-prod/amazon/orders/` (Parquet)           |
| Target      | `s3://etl-agent-artifacts-prod/analytics/rfm/` (Delta Lake)  |
| Format      | Delta Lake (overwrite)                                       |
| Partitioning| None                                                         |
| PySpark     | 3.5+                                                         |
| Delta Lake  | 3.x                                                          |

Reads raw Amazon order transactions from S3, computes **Recency**,
**Frequency**, and **Monetary** scores per customer using quintile ranking,
and writes the enriched result as a Delta Lake table ready for downstream
analytics and CRM activation.

---

## Pipeline Architecture
