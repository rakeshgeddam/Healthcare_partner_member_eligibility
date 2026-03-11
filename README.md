# Healthcare Eligibility Pipeline (Databricks)
Configuration-Driven Ingestion using Medallion Architecture (Bronze → Silver → Gold)

This project is a scalable Databricks (PySpark) pipeline that ingests member eligibility files from multiple healthcare partners—even when each partner uses different file formats and column names. It's configuration-driven, so onboarding a new partner usually requires only inserting config/mapping rows (no code changes).

## What this project does
- Ingests partner files (CSV/TXT, different delimiters) into a raw staging layer.
- Validates and flags bad records (e.g., missing external_id, invalid DOB/email).
- Produces a final standardized "production" table with cleaned/consistent fields.

Use cases: fast onboarding of new data partners, reliable data quality checks, and a repeatable ETL pattern for eligibility operations.

## Architecture (simple explanation)
This repo follows the Databricks medallion pattern: **raw → clean → trusted**.

- **Configuration Layer**
  - `partner_config`: tells the system how to read each partner file (format, delimiter, etc.).
  - `partner_column_mapping`: maps each partner's column names to the pipeline's standard column names.
- **Bronze (Raw Ingestion) — `Bronze.ipynb`**
  - Reads partner files using `partner_config`.
  - Dynamically maps columns using `partner_column_mapping`.
  - Writes to `member_eligibility_staging`.
- **Silver (Validation & Data Quality) — `Silver.ipynb`**
  - Validates fields like `external_id`, DOB format, and email format.
  - Writes invalid records to `member_eligibility_rejected` (with error details).
  - Updates validation status in the staging table.
- **Gold (Final Output) — `Gold.ipynb`**
  - Filters only valid records.
  - Standardizes/cleans fields and writes to the final table `member_eligibility`.

## Prerequisites
- Databricks workspace with permissions to create catalogs/schemas/tables.
- Unity Catalog enabled.
