
# 📁 Data Documentation

## Data Files Not Included

Data files are excluded from this repository to keep it lightweight and follow best practices.

---

## Required Data Structure
## How to Get Data

**Download from**: [Medium Article - End To End DE Project Using Snowpark](https://data-engineering-simplified.medium.com/end-to-end-de-project-using-snowpark-amazon-sales-analytics-744c1d5a8d50)

**Steps**:
1. Download sample data from the article above
2. Extract to `end2end-sample-data/` folder
3. Run `python3 data_loading.py`

## Data Schema

### Sales CSV Files

Required columns:
ORDER_ID, ORDER_DT, CUSTOMER_NAME, MOBILE_KEY, ORDER_QUANTITY,
PRICE_PER_UNIT, TOTAL_PRICE, PROMOTION_CODE, FINAL_ORDER_AMOUNT,
TAX_AMOUNT, PAYMENT_STATUS, SHIPPING_STATUS, PAYMENT_METHOD,
PAYMENT_PROVIDER, CONCTACT_NO, SHIPPING_ADDRESS

### Exchange Rate CSV

Required columns:DATE, USD2USD, USD2EUR, USD2INR, USD2CAD, USD2UK, USD2JP

## Expected Data Volume

| Country | Files | Rows |
|---------|-------|------|
| India   | 31    | ~33,911 |
| USA     | 30    | ~22,575 |
| France  | 30    | ~18,763 |
| **Total** | **91** | **~75,249** |

---

## After Getting Data
1. Place files in end2end-sample-data/
2. Activate environment
source .venv/bin/activate

3. Upload to Snowflake
python3 data_loading.py
python3 upload_exchange_rate.py

4. Run pipeline
python3 stage2source.py
python3 source2curated.py
python3 curated2model.py