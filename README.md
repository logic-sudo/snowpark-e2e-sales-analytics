# Snowflake End-to-End Data Engineering Project

## Amazon Sales Analytics Pipeline

Complete data engineering project using Snowflake and Snowpark Python.

### Architecture
- **SOURCE**: 75,249 rows
- **CURATED**: 14,127 rows  
- **CONSUMPTION**: Star schema with 6 dimensions + 1 fact table

### Technologies
- Snowflake
- Snowpark Python
- Pandas
- SQL


## 📝 Python Scripts Detailed Description

### 1. **connectivity.py**
**Purpose**: Snowflake session and connection management

**Key Functions**:
- Creates reusable Snowpark session objects
- Manages connection parameters (account, user, password, warehouse)
- Handles authentication and session lifecycle
- Provides connection pooling for efficient resource usage


---

### 2. **data_loading.py**
**Purpose**: Upload raw sales data files to Snowflake internal stage

**What it does**:
- Reads local CSV/Parquet/JSON files from `end2end-sample-data/` folder
- Uploads files to Snowflake internal stage `@my_internal_stg`
- Organizes files by country: `sales/source=IN/`, `sales/source=US/`, `sales/source=FR/`
- Validates file uploads and reports status

**Data loaded**:
- India sales: 31 files → 33,911 rows
- USA sales: 30 files → 22,575 rows
- France sales: 30 files → 18,763 rows

**Command**: `python3 data_loading.py`

---

### 3. **upload_exchange_rate.py**
**Purpose**: Load currency exchange rate reference data

**What it does**:
- Uploads exchange rate CSV file to internal stage
- Loads data into `common.exchange_rate` table
- Provides USD conversion rates for: INR, EUR, CAD, GBP, JPY
- Used for multi-currency sales analysis

**Exchange rates loaded**: USD to 6+ currencies

**Command**: `python3 upload_exchange_rate.py`

---

### 4. **stage2source.py**
**Purpose**: ETL - Load data from Internal Stage to SOURCE schema

**Transformations**:
- Reads files from internal stage (`@my_internal_stg`)
- Applies file format definitions (CSV, Parquet, JSON)
- Loads data into SOURCE tables:
  - `source.in_sales_order` (India - 33,911 rows)
  - `source.us_sales_order` (USA - 22,575 rows)
  - `source.fr_sales_order` (France - 18,763 rows)
- Handles data type conversions
- Manages COPY INTO operations with error handling

**Total loaded**: 75,249 rows across 3 countries

**Command**: `python3 stage2source.py`

---

### 5. **source2curated.py**
**Purpose**: ETL - Transform SOURCE to CURATED schema (data cleaning & enrichment)

**Transformations**:
1. **Data Filtering**:
   - Filters only `PAYMENT_STATUS = 'Paid'` records
   - Filters only `SHIPPING_STATUS = 'Delivered'` records
   
2. **Data Enrichment**:
   - Adds `Country` column (India/USA/France)
   - Adds `Region` column (Asia/North America/Europe)
   - Adds `LOCAL_CURRENCY` column (INR/USD/EUR)
   
3. **Currency Conversion**:
   - Joins with exchange rate table
   - Converts local amounts to USD using `EXCHANGE_RATE`
   - Calculates `US_TOTAL_ORDER_AMT` and `USD_TAX_AMT`
   
4. **Time Dimensions**:
   - Extracts `ORDER_YEAR`, `ORDER_MONTH`, `ORDER_QUARTER`
   - Enables time-based analytics
   
5. **Deduplication**:
   - Uses window functions to remove duplicate orders
   - Ranks by order date and metadata timestamp

**Output**: 14,127 clean, enriched rows in CURATED schema

**Command**: `python3 source2curated.py`

---

### 6. **curated2model.py**
**Purpose**: ETL - Build CONSUMPTION star schema from CURATED data

**Star Schema Components**:

#### **Dimension Tables Created** (SCD Type 2):
1. **region_dim**: Country and region combinations
2. **product_dim**: Product catalog (Brand/Model/Color/Memory)
   - Splits `mobile_key` → `Brand`, `Model`, `Color`, `Memory`
3. **promo_code_dim**: Promotion codes by country/region
4. **customer_dim**: Customer details with address
5. **payment_dim**: Payment methods and providers
6. **date_dim**: Calendar table with date attributes
   - Generates dates using SQL GENERATOR function
   - Includes: year, month, quarter, day, weekday/weekend

#### **Fact Table**:
- **sales_fact**: Central fact table with foreign keys
  - Links to all 6 dimension tables
  - Contains measures: quantities, amounts, exchange rates
  - Implements star schema for optimized analytics

**What it does**:
- Extracts distinct dimension values from curated data
- Implements **incremental loading** (only new records)
- Uses **leftanti joins** to prevent duplicates
- Generates surrogate keys using sequences
- Creates star schema with proper foreign key relationships

**Output**: 14,127 fact records + dimension records

**Command**: `python3 curated2model.py`

---

### 7. **test_curated2model.py**
**Purpose**: Unit tests for data transformation logic

**What it tests**:
- Data type validations
- Transformation accuracy
- Dimension table integrity
- Foreign key relationships
- Data quality checks
- Row count validations

**Command**: `python3 test_curated2model.py`

---

## 📊 Data Flow Summary

| Pipeline Stage | Script | Input | Output | Row Count |
|---------------|--------|-------|--------|-----------|
| **Stage 1: Upload** | `data_loading.py` | Local files | Internal Stage | 91 files |
| **Stage 2: Extract** | `stage2source.py` | Internal Stage | SOURCE schema | 75,249 rows |
| **Stage 3: Transform** | `source2curated.py` | SOURCE | CURATED schema | 14,127 rows |
| **Stage 4: Model** | `curated2model.py` | CURATED | CONSUMPTION schema | 14,127 rows (fact) |

---

## 📋 Prerequisites

- Snowflake account (free trial: https://signup.snowflake.com)
- Python 3.9+
- Virtual environment (`venv`)
- Sample data files

## 🚀 Installation & Setup

### 1. Clone Repository
### 2. Create Virtual Environment
### 3. Install Dependencies
### 4. Configure Snowflake Credentials
### 5. Create Snowflake Database & Schemas


**Expected Runtime**: ~2-3 minutes for complete pipeline

---

## 📈 Consumption Layer Schema

### Dimension Tables

| Table | Purpose | Key Attributes | SCD Type |
|-------|---------|---------------|----------|
| `region_dim` | Geographic regions | country, region | Type 2 |
| `product_dim` | Product catalog | brand, model, color, memory | Type 2 |
| `promo_code_dim` | Promotion codes | promotion_code, country | Type 2 |
| `customer_dim` | Customer info | name, contact, address | Type 2 |
| `payment_dim` | Payment methods | method, provider | Type 2 |
| `date_dim` | Calendar table | year, month, quarter, weekday | - |

### Fact Table

**`sales_fact`** - Grain: One row per order
- **Foreign Keys**: Links to all 6 dimensions
- **Measures**: 
  - `order_quantity`
  - `local_total_order_amt`
  - `usd_order_amount`
  - `exchange_rate`
  - `local_tax_amt`, `usd_tax_amt`

---

## 📊 Snowflake Dashboard

### KPIs & Visualizations

1. **Total Revenue**: 87,000 USD
2. **Revenue by Country**: Bar chart (India leads with 6K)
3. **Sales Trend**: Time series showing daily patterns
4. **Promo Code Effectiveness**: Area chart tracking promo usage
5. **Payment Method Analysis**: Distribution of payment types

### Filters Available
- Country: India, USA, France
- Fiscal Year: 2020
- Date Range: Custom date selection

---

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| **Snowflake** | Cloud data warehouse platform |
| **Snowpark Python** | Data transformation framework |
| **Pandas** | Data manipulation library |
| **Python 3.9** | Programming language |
| **SQL** | Query & DDL operations |
| **Git** | Version control |

---

## 📚 Key Concepts Demonstrated

1. **Medallion Architecture** (Bronze/Silver/Gold)
2. **Star Schema Design** (Kimball methodology)
3. **Slowly Changing Dimensions (SCD Type 2)**
4. **ETL Pipeline Development**
5. **Data Quality & Validation**
6. **Incremental Data Loading**
7. **Currency Conversion & Normalization**
8. **Time-based Analytics**
9. **Dashboard Development**

---

## 📝 Project Learnings

- ✅ Snowflake architecture and storage optimization
- ✅ Snowpark DataFrame API and transformations
- ✅ Dimensional modeling best practices
- ✅ ETL pipeline error handling
- ✅ Data validation techniques
- ✅ Snowsight dashboard creation
- ✅ Performance tuning with clustering keys

---

## 📚 References

- [Snowflake Documentation](https://docs.snowflake.com)
- [Snowpark Python Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html)
- [Medium Article Reference](https://data-engineering-simplified.medium.com/end-to-end-de-project-using-snowpark-amazon-sales-analytics-744c1d5a8d50)

---

## 👤 Author

**Kshitij Kharche**
- GitHub: https://github.com/logic-sudo
- LinkedIn: https://www.linkedin.com/in/kshitijkharche
- Email: kshitijkharche22@gmail.com

---

## ⭐ Show Your Support

Give a ⭐️ if this project helped you learn Snowflake & Data Engineering!

---

## 🔮 Future Enhancements

- [ ] Add incremental data loading schedules
- [ ] Implement data quality monitoring
- [ ] Add real-time streaming ingestion
- [ ] Create email alerts for KPI thresholds
- [ ] Integrate with external BI tools (Tableau/Power BI)
- [ ] Add machine learning forecasting models
- [ ] Implement CI/CD pipeline for automated testing

### Dashboard
Interactive Snowflake dashboard with KPIs and filters.
