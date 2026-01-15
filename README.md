# 🏥 Healthcare Eligibility Pipeline

> **Configuration-Driven Data Ingestion** for Healthcare Partner Member Eligibility

A scalable, medallion architecture data pipeline that ingests member eligibility files from multiple healthcare partners with varying file formats. The solution uses a configuration-driven approach built on **Databricks/PySpark**.

**Architecture:** Bronze → Silver → Gold

---

## 📋 Table of Contents

- [Solution Architecture](#solution-architecture)
- [Prerequisites](#prerequisites)
- [Database Schema](#database-schema)
- [Setup Instructions](#setup-instructions)
- [Running the Pipeline](#running-the-pipeline)
- [Adding a New Partner](#adding-a-new-partner)
- [Validation & Error Handling](#validation--error-handling)
- [Output Schema](#output-schema)
- [Technical Highlights](#technical-highlights)
- [File Structure](#file-structure)

---

## 🏗️ Solution Architecture

### Medallion Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    CONFIGURATION LAYER                       │
│  • partner_config (file format, delimiter)                  │
│  • partner_column_mapping (source → target mapping)         │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│  🥉 BRONZE: Raw Data Ingestion (Bronze.ipynb)               │
│  • Reads partner files based on configuration               │
│  • Maps columns dynamically                                 │
│  • Writes to: member_eligibility_staging                    │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│  🥈 SILVER: Data Quality & Validation (Silver.ipynb)        │
│  • Validates external_id, dob format, email format          │
│  • Flags invalid records with error details                 │
│  • Writes to: member_eligibility_rejected                   │
│  • Updates staging table validation status                  │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│  🥇 GOLD: Final Standardized Output (Gold.ipynb)            │
│  • Filters only valid records                               │
│  • Standardizes schema and applies transformations          │
│  • Writes to: member_eligibility (production table)         │
└─────────────────────────────────────────────────────────────┘
```

---

## ✅ Prerequisites

- **Databricks Workspace** with access to create catalogs, schemas, and tables
- **Unity Catalog** enabled
- **Volumes** configured for file storage
- **Permissions:** CREATE TABLE, READ/WRITE access to `eligibility_operation` catalog

---

## 📊 Database Schema

### Configuration Tables

#### 1. `partner_config`
Stores partner-level metadata for file ingestion.

| Column | Type | Description |
|--------|------|-------------|
| `partner_id` | INT | Unique partner identifier (Primary Key) |
| `partner_code` | STRING | Short code (e.g., 'acme', 'bettercare') |
| `partner_name` | STRING | Full partner name |
| `file_delimiter` | STRING | File delimiter ('|', ',', etc.) |
| `file_format` | STRING | File format ('TXT' or 'CSV') |

#### 2. `partner_column_mapping`
Maps source columns to standardized target fields.

| Column | Type | Description |
|--------|------|-------------|
| `partner_id` | INT | Foreign key to partner_config |
| `source_column_name` | STRING | Column name in partner file |
| `target_field_name` | STRING | Standardized field name |

---

### Data Tables

#### 3. `member_eligibility_staging`
Bronze/Silver staging table with validation status.

| Column | Type | Description |
|--------|------|-------------|
| `staging_id` | LONG | Auto-generated ID |
| `partner_id` | INT | Partner identifier |
| `partner_code` | STRING | Partner code |
| `external_id` | STRING | Member identifier from partner |
| `firstname` | STRING | First name |
| `lastname` | STRING | Last name |
| `dob` | STRING | Date of birth (raw format) |
| `email` | STRING | Email address |
| `phone` | STRING | Phone number |
| `validation_status` | STRING | 'PENDING' or 'FINISHED' |
| `validation_errors` | STRING | Semicolon-separated error fields |
| `created_date` | TIMESTAMP | Record creation time |
| `load_date` | TIMESTAMP | Batch load time |

#### 4. `member_eligibility_rejected`
Stores invalid records for auditing.

| Column | Type | Description |
|--------|------|-------------|
| `reject_id` | LONG | Auto-generated ID |
| `partner_id` | INT | Partner identifier |
| `partner_code` | STRING | Partner code |
| `external_id_sample` | STRING | Sample of external_id |
| `error_codes` | STRING | Comma-separated error codes |
| `field_errors` | STRING | Semicolon-separated field names |
| `rejected_date` | TIMESTAMP | Rejection timestamp |

#### 5. `member_eligibility` (Gold)
Final production table with standardized, validated data.

| Column | Type | Description |
|--------|------|-------------|
| `eligibility_id` | LONG | Auto-generated surrogate key |
| `external_id` | STRING | Member identifier |
| `firstname` | STRING | Title case |
| `lastname` | STRING | Title case |
| `dob` | DATE | ISO-8601 format (YYYY-MM-DD) |
| `email` | STRING | Lowercase |
| `phone` | STRING | Formatted (XXX-XXX-XXXX) |
| `partner_code` | STRING | Partner identifier |
| `load_date` | TIMESTAMP | Load timestamp |
| `partner_id` | INT | Partner ID |

---

## 🚀 Setup Instructions

### Step 1: Create Catalog and Schema

```sql
CREATE CATALOG IF NOT EXISTS eligibility_operation;
USE CATALOG eligibility_operation;
CREATE SCHEMA IF NOT EXISTS default;
USE SCHEMA default;
```

### Step 2: Create Configuration Tables

Run the DDL scripts to create:
- `partner_config`
- `partner_column_mapping`
- `data_quality_metadata` (optional)

### Step 3: Create Data Tables

```sql
-- See Tables_DDL.sql for full schema definitions
-- Creates:
--   - member_eligibility_staging
--   - member_eligibility_rejected
--   - member_eligibility
```

### Step 4: Set Up Volumes for File Storage

```sql
CREATE VOLUME IF NOT EXISTS eligibility_operation.default.input_files;
CREATE VOLUME IF NOT EXISTS eligibility_operation.default.output_files;
```

Upload partner files to:
```
/Volumes/eligibility_operation/default/input_files/<partner_code>/
```

### Step 5: Insert Initial Partner Configurations

See [Adding a New Partner](#adding-a-new-partner) section below.

---

## ▶️ Running the Pipeline

### Option 1: Run Notebooks Sequentially

Execute notebooks in order:

#### 1. **Bronze.ipynb** - Ingests raw data
```python
# Widget parameters:
# - partner: "acme" or "bettercare"
# - catalog: "eligibility_operation"
# - schema: "default"
```

#### 2. **Silver.ipynb** - Validates and cleanses
```python
# Automatically processes all PENDING records
# No parameters needed
```

#### 3. **Gold.ipynb** - Produces final output
```python
# Writes validated records to production table
```

### Option 2: Delta Live Tables Pipeline (Recommended)

Configure a DLT pipeline with dependencies:
```
Bronze → Silver → Gold
```

Set schedule or trigger on file arrival.

---

## ➕ Adding a New Partner

**Example:** Onboarding "HealthCare Plus" (partner_id = 3)

### Step 1: Add Partner Configuration

```sql
INSERT INTO eligibility_operation.default.partner_config
VALUES (
  3,                          -- partner_id
  'healthcareplus',          -- partner_code
  'HealthCare Plus Corp',    -- partner_name
  ',',                       -- file_delimiter (CSV)
  'CSV'                      -- file_format
);
```

### Step 2: Add Column Mappings

```sql
INSERT INTO eligibility_operation.default.partner_column_mapping VALUES
(3, 'member_number', 'external_id'),
(3, 'first_name', 'firstname'),
(3, 'last_name', 'lastname'),
(3, 'birth_date', 'dob'),
(3, 'email_address', 'email'),
(3, 'phone_number', 'phone');
```

### Step 3: Upload Data File

Place the partner file in:
```
/Volumes/eligibility_operation/default/input_files/healthcareplus/data.csv
```

### Step 4: Run Bronze Notebook

Set widget parameter:
```python
partner = "healthcareplus"
```

**That's it! No code changes required.** ✨

---

## 🛡️ Validation & Error Handling

### Validation Rules

| Rule | Error Code | Description |
|------|-----------|-------------|
| External ID required | `EXT_NULL` | external_id is null or empty |
| DOB format | `DOB_FMT` | dob not in YYYY-MM-DD format |
| Email format | `EMAIL_FMT` | Invalid email pattern |

### Error Handling Flow

1. Silver layer validates all staging records
2. Invalid records → `member_eligibility_rejected` table
3. Valid records → marked `validation_status = 'FINISHED'`
4. Gold layer only processes valid records

### Sample Error Output

```
field_errors: "external_id; dob"
error_codes: "EXT_NULL,DOB_FMT"
```

---

## 📤 Output Schema

### Transformations Applied

| Field | Transformation |
|-------|---------------|
| `firstname` | Title Case |
| `lastname` | Title Case |
| `dob` | Convert to DATE (YYYY-MM-DD) |
| `email` | Lowercase |
| `phone` | Format as XXX-XXX-XXXX |
| `partner_code` | From configuration |
| `load_date` | Current timestamp |

---

## 🌟 Technical Highlights

✅ **Configuration-driven:** New partners require only config updates  
✅ **Medallion architecture:** Bronze → Silver → Gold separation  
✅ **Data quality validation:** Comprehensive error tracking  
✅ **Idempotent:** Rerunnable without duplicates  
✅ **Scalable:** Works with any file format/delimiter  
✅ **Audit trail:** Rejected records preserved with reasons  

---

## 📁 File Structure

```
├── Bronze.ipynb              # Raw data ingestion
├── Silver.ipynb              # Validation & quality checks
├── Gold.ipynb                # Final output generation
├── Tables_DDL.sql            # Table creation scripts
├── README.md                 # This file
└── sample_data/
    ├── acme.txt              # Sample pipe-delimited file
    └── bettercare.csv        # Sample comma-delimited file
```

---

## 📝 License

This project is licensed under the MIT License.

## 🤝 Contributing

Contributions, issues, and feature requests are welcome!

---

**Built with ❤️ using Databricks and PySpark**