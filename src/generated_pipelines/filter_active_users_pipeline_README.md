# filter_active_users_pipeline

## Overview

| Property    | Value                                          |
|-------------|------------------------------------------------|
| Pipeline    | `filter_active_users_pipeline`                 |
| Description | Filter users parquet data to retain only active users and write to the processed bucket. |
| Operations  | `filter`                                       |
| Source      | `s3://etl-agent-raw/users/` (parquet)          |
| Target      | `s3://etl-agent-processed/active-users/` (parquet) |
| Write Mode  | `overwrite`                                    |

---

## Pipeline Architecture
