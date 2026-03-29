# RFM Customer Segmentation Pipeline

## Overview

| Property    | Value                                                        |
|-------------|--------------------------------------------------------------|
| Pipeline ID | `rfm_customer_segmentation_pipeline`                         |
| Owner       | Data Engineering                                             |
| Runtime     | PySpark 3.5+ / Delta Lake                                    |
| Schedule    | Daily (recommended)                                          |
| Source      | `s3://etl-agent-raw-prod/amazon/orders/` (Parquet)           |
| Target      | `s3://etl-agent-artifacts-prod/analytics/rfm/` (Delta Lake)  |

Reads raw Amazon order transactions from S3, computes **Recency**,
**Frequency**, and **Monetary** scores per customer using quintile ranking,
and writes the enriched result as a Delta Lake table ready for downstream
analytics and CRM activation.

---

## Architecture
