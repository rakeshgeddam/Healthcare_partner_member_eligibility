

# Healthcare Eligibility Pipeline (Databricks)  
Configuration‑Driven Ingestion using Medallion Architecture (Bronze → Silver → Gold) [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

This project is a scalable Databricks (PySpark) pipeline that ingests member eligibility files from multiple healthcare partners—even when each partner uses different file formats and column names.  It’s configuration‑driven, so onboarding a new partner usually requires only inserting config/mapping rows (no code changes). [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

## What this project does
- Ingests partner files (CSV/TXT, different delimiters) into a raw staging layer. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
- Validates and flags bad records (e.g., missing external_id, invalid DOB/email). [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
- Produces a final standardized “production” table with cleaned/consistent fields. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

Use cases: fast onboarding of new data partners, reliable data quality checks, and a repeatable ETL pattern for eligibility operations. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

## Architecture (simple explanation)
This repo follows the Databricks medallion pattern: **raw → clean → trusted**. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

- **Configuration Layer**  
  - `partner_config`: tells the system how to read each partner file (format, delimiter, etc.). [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
  - `partner_column_mapping`: maps each partner’s column names to the pipeline’s standard column names. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

- **Bronze (Raw Ingestion) — `Bronze.ipynb`**  
  - Reads partner files using `partner_config`. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
  - Dynamically maps columns using `partner_column_mapping`. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
  - Writes to `member_eligibility_staging`. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

- **Silver (Validation & Data Quality) — `Silver.ipynb`**  
  - Validates fields like `external_id`, DOB format, and email format. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
  - Writes invalid records to `member_eligibility_rejected` (with error details). [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
  - Updates validation status in the staging table. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

- **Gold (Final Output) — `Gold.ipynb`**  
  - Filters only valid records. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
  - Standardizes/cleans fields and writes to the final table `member_eligibility`. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

## Prerequisites
- Databricks workspace with permissions to create catalogs/schemas/tables. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
- Unity Catalog enabled. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
- Volumes enabled for file storage. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
- Permissions: CREATE TABLE + read/write access to the `eligibility_operation` catalog. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

## Setup (step‑by‑step)
Follow these steps once to initialize the environment. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

### Step 1: Create catalog + schema
Run in a Databricks SQL editor: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

```sql
CREATE CATALOG IF NOT EXISTS eligibility_operation;

USE CATALOG eligibility_operation;

CREATE SCHEMA IF NOT EXISTS default;

USE SCHEMA default;
```


### Step 2: Create tables (DDL)
Create the configuration + data tables (recommended to run the DDL scripts included in this repo). [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
The project expects tables like: `partner_config`, `partner_column_mapping`, `member_eligibility_staging`, `member_eligibility_rejected`, and `member_eligibility`. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

> Tip: If you have `Tables_DDL.sql` in the repo, run it end-to-end in Databricks SQL. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

### Step 3: Create volumes for files
Run in Databricks SQL: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

```sql
CREATE VOLUME IF NOT EXISTS eligibility_operation.default.input_files;
CREATE VOLUME IF NOT EXISTS eligibility_operation.default.output_files;
```


### Step 4: Upload partner input files to Volumes
Place partner files under this path (folder name must match the partner code): [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

```text
/Volumes/eligibility_operation/default/input_files/<partner_code>/
```


Example: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

```text
/Volumes/eligibility_operation/default/input_files/acme/acme.txt
/Volumes/eligibility_operation/default/input_files/bettercare/bettercare.csv
```


### Step 5: Insert partner configuration + mappings
You must insert: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
- 1 row in `partner_config` per partner [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
- multiple rows in `partner_column_mapping` per partner (one per column mapping) [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

See “Adding a New Partner” below for a ready-to-copy example. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

## Running the pipeline
Two ways to run: notebooks (simple) or Delta Live Tables (recommended). [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

### Option A: Run notebooks sequentially (easy)
Run these notebooks in order: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

1) **Bronze — `Bronze.ipynb`** (ingests raw data) [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
Use widgets/parameters like: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

```python
# Widget parameters (example)
# - partner: "acme" or "bettercare"
# - catalog: "eligibility_operation"
# - schema: "default"
```


2) **Silver — `Silver.ipynb`** (validates + rejects invalid rows) [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
Silver processes all records with `validation_status = 'PENDING'` (no parameters needed). [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

3) **Gold — `Gold.ipynb`** (creates final standardized output) [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
Gold writes validated records into the production table. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

### Option B: Delta Live Tables (recommended)
Configure a DLT pipeline with this dependency order: `Bronze → Silver → Gold`. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)
You can schedule it or trigger on file arrival for more automation. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

## Adding a new partner (no code changes)
Example onboarding: “HealthCare Plus” (partner_id = 3). [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

### 1) Add the partner record
```sql
INSERT INTO eligibility_operation.default.partner_config
VALUES (
  3,                 -- partner_id
  'healthcareplus',  -- partner_code
  'HealthCare Plus Corp', -- partner_name
  ',',               -- file_delimiter (CSV)
  'CSV'              -- file_format
);
```


### 2) Add column mappings (source → standard)
```sql
INSERT INTO eligibility_operation.default.partner_column_mapping
VALUES
(3, 'member_number', 'external_id'),
(3, 'first_name', 'firstname'),
(3, 'last_name', 'lastname'),
(3, 'birth_date', 'dob'),
(3, 'email_address', 'email'),
(3, 'phone_number', 'phone');
```


### 3) Upload the partner file
```text
/Volumes/eligibility_operation/default/input_files/healthcareplus/data.csv
```


### 4) Run Bronze with the partner widget
```python
partner = "healthcareplus"
```


That’s it—no code changes required because ingestion + mapping are driven by configuration tables. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

## Data quality, errors, and output
### Validation rules
Silver validates records and labels errors using codes like these: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

| Rule | Error code | Meaning |
|---|---|---|
| External ID required | EXT_NULL | `external_id` is missing/empty.  [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf) |
| DOB format | DOB_FMT | `dob` not in `YYYY-MM-DD`.  [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf) |
| Email format | EMAIL_FMT | Invalid email pattern.  [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf) |

Invalid rows go to `member_eligibility_rejected`, and valid rows are marked `validation_status = 'FINISHED'`.  Gold only processes valid records. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

### Transformations (Gold output)
Gold standardizes the final dataset like this: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf)

| Field | Transformation |
|---|---|
| `firstname`, `lastname` | Title Case.  [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf) |
| `dob` | Converted to DATE (`YYYY-MM-DD`).  [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf) |
| `email` | Lowercase.  [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf) |
| `phone` | Formatted as `XXX-XXX-XXXX`.  [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf) |
| `partner_code` | Derived from configuration.  [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf) |
| `load_date` | Current timestamp.  [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/120761757/c5197ff0-7946-4341-9f7f-d4b55067218a/Readme.pdf) |

```
