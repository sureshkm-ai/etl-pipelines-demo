# Pipeline: `revenue_report_for_completed_transactions`

## Overview

| Property        | Value                                                                                      |
|-----------------|--------------------------------------------------------------------------------------------|
| **Pipeline**    | `revenue_report_for_completed_transactions`                                                |
| **Source**      | `s3://etl-agent-raw-prod/olist/orders/` (CSV)                                              |
| **Target**      | `s3://etl-agent-processed-production/revenue_report_for_completed_transactions/` (Delta)   |
| **Format**      | Delta Lake (overwrite, partitioned by `year` / `month`)                                    |
| **PySpark**     | 3.5+                                                                                       |
| **Delta Lake**  | 3.x                                                                                        |

Filters the Olist orders dataset to include **only delivered orders**, removes rows
with null `order_id`s, and partitions the output by the year and month derived from
`order_purchase_timestamp`.

---

## Architecture
