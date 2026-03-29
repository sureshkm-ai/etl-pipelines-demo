# rfm_customer_segmentation_pipeline

## Overview

This PySpark pipeline reads raw e-commerce order transactions from Amazon S3,
computes **Recency**, **Frequency**, and **Monetary (RFM)** scores per customer,
and writes the enriched result as a **Delta Lake** table for downstream analytics.

---

## Architecture
