# geographic_revenue_analysis_by_state_pipeline

## Overview

Aggregates monthly **iPhone 17** campaign revenue by US state to identify
top-performing regions. The pipeline reads raw Amazon order data from S3,
filters to iPhone 17 SKUs, computes per-state monthly revenue and order
counts, ranks states by revenue, and persists the results as a partitioned
Delta Lake table.

---

## Architecture
