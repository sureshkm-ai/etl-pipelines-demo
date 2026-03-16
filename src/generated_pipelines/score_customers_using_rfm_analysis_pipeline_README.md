# score_customers_using_rfm_analysis_pipeline

## Overview

This pipeline computes **RFM (Recency, Frequency, Monetary)** scores for every
customer in the Amazon Orders dataset. Scores are derived using **quintile
bucketing (ntile 1–5)** and combined into a single `rfm_score` that drives
segment labelling for targeted marketing campaigns.

---

## Architecture
