# Pipeline: `rfm_customer_segmentation_iphone17_campaign`

## Overview

| Property        | Value                                                                                      |
|-----------------|--------------------------------------------------------------------------------------------|
| **Pipeline ID** | `rfm_customer_segmentation_iphone17_campaign`                                              |
| **Source**      | `s3://etl-agent-raw-prod/olist/orders/` (CSV, no header, 8 columns)                       |
| **Target**      | `s3://etl-agent-processed-production/rfm_customer_segmentation_iphone17_campaign/` (Delta) |
| **Format**      | Delta Lake (overwrite mode)                                                                |
| **Partitioning**| `year`, `month` (derived from `order_purchase_timestamp`)                                  |
| **PySpark**     | 3.5+                                                                                       |
| **Delta Lake**  | 3.x                                                                                        |

---

## Description

Filters the Olist e-commerce orders dataset to retain only **delivered** orders,
removes rows with null `order_id`, derives year/month partition columns from
`order_purchase_timestamp`, and writes the cleaned dataset to Delta Lake.
This output feeds downstream RFM (Recency–Frequency–Monetary) customer
segmentation for the iPhone 17 marketing campaign.

---

## Source Schema

The raw CSV has **no header row**. Spark reads columns as `_c0`–`_c7` (all `string`),
which are immediately renamed to their semantic equivalents:

| Raw Column | Semantic Name                    | Type (after cast) |
|------------|----------------------------------|-------------------|
| `col0`     | `order_id`                       | `string`          |
| `col1`     | `customer_id`                    | `string`          |
| `col2`     | `order_status`                   | `string`          |
| `col3`     | `order_purchase_timestamp`       | `timestamp`       |
| `col4`     | `order_approved_at`              | `string`          |
| `col5`     | `order_delivered_carrier_date`   | `string`          |
| `col6`     | `order_delivered_customer_date`  | `string`          |
| `col7`     | `order_estimated_delivery_date`  | `string`          |

---

## Target Schema

| Column                           | Type        | Notes                          |
|----------------------------------|-------------|--------------------------------|
| `order_id`                       | `string`    | Non-null, deduplicated PK      |
| `customer_id`                    | `string`    | Nulls filled with `"UNKNOWN"`  |
| `order_status`                   | `string`    | Always `"delivered"`           |
| `order_purchase_timestamp`       | `timestamp` | Cast from string               |
| `order_approved_at`              | `string`    | Nulls filled with `"UNKNOWN"`  |
| `order_delivered_carrier_date`   | `string`    | Nulls filled with `"UNKNOWN"`  |
| `order_delivered_customer_date`  | `string`    | Nulls filled with `"UNKNOWN"`  |
| `order_estimated_delivery_date`  | `string`    | Nulls filled with `"UNKNOWN"`  |
| `year`                           | `integer`   | Partition column               |
| `month`                          | `integer`   | Partition column               |

---

## Transformation Steps
