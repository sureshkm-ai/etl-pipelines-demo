# RFM Customer Segmentation – iPhone 17 Campaign Pipeline

## Overview

| Property    | Value                                                                 |
|-------------|-----------------------------------------------------------------------|
| Pipeline ID | `rfm_customer_segmentation_iphone17_campaign_pipeline`               |
| Purpose     | Compute RFM scores for Amazon customers to identify high-value targets for the iPhone 17 launch campaign |
| Source      | `s3://etl-agent-raw-prod/amazon/orders/` (Parquet)                   |
| Target      | `s3://etl-agent-artifacts-prod/analytics/rfm_scores/` (Delta Lake)   |
| Write Mode  | Overwrite (schema evolution enabled)                                  |
| PySpark     | 3.5+                                                                  |
| Delta Lake  | ✅                                                                    |

---

## Pipeline Architecture
