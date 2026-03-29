# rfm_customer_segmentation_pipeline

## Overview

This pipeline reads raw e-commerce order transactions from Amazon S3 (Parquet),
computes **Recency**, **Frequency**, and **Monetary** (RFM) metrics per customer,
assigns quintile-based scores, classifies each customer into a named segment, and
persists the result as a **Delta Lake** table.

---

## Architecture
