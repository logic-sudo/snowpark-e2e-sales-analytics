import sys
import logging
import pandas as pd

from snowflake.snowpark import Session, DataFrame, CaseExpr
from snowflake.snowpark.functions import col, lit, row_number, rank, split, cast, when, expr, min, max
from snowflake.snowpark.types import StructType, StringType, StructField, LongType, DecimalType, DateType, TimestampType, IntegerType
from snowflake.snowpark import Window

# Initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

# Snowpark session
def get_snowpark_session() -> Session:
    connection_parameters = {
        "ACCOUNT": "QNNERMQ-RZ07987",
        "USER": "snowpark_user",
        "PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", "YOUR_PASSWORD_HERE"),
        "ROLE": "SYSADMIN",
        "DATABASE": "sales_dwh",
        "WAREHOUSE": "SNOWPARK_ETL_WH"
    }
    return Session.builder.configs(connection_parameters).create()   

# Region Dimension
def create_region_dim(all_sales_df, session) -> None:
    logging.info("Creating Region Dimension...")
    
    region_dim_df = all_sales_df.groupBy(col("Country"), col("Region")).count()
    region_dim_df = region_dim_df.with_column("isActive", lit('Y'))
    region_dim_df = region_dim_df.selectExpr("sales_dwh.consumption.region_dim_seq.nextval as region_id_pk", "Country", "Region", "isActive") 
    
    existing_region_dim_df = session.sql("select Country, Region from sales_dwh.consumption.region_dim")
    region_dim_df = region_dim_df.join(existing_region_dim_df, ["Country", "Region"], join_type='leftanti')
    
    insert_cnt = int(region_dim_df.count())
    if insert_cnt > 0:
        region_dim_df.write.save_as_table("sales_dwh.consumption.region_dim", mode="append")
        logging.info(f"✓ Region Dimension: {insert_cnt} rows inserted")
    else:
        logging.info("✓ Region Dimension: No new records to insert")

# Product Dimension
def create_product_dim(all_sales_df, session) -> None:
    logging.info("Creating Product Dimension...")
    
    product_dim_df = all_sales_df.with_column("Brand", split(col('MOBILE_KEY'), lit('/'))[0]) \
                                .with_column("Model", split(col('MOBILE_KEY'), lit('/'))[1]) \
                                .with_column("Color", split(col('MOBILE_KEY'), lit('/'))[2]) \
                                .with_column("Memory", split(col('MOBILE_KEY'), lit('/'))[3]) \
                                .select(col('mobile_key'), col('Brand'), col('Model'), col('Color'), col('Memory'))
    
    product_dim_df = product_dim_df.select(
        col('mobile_key'),
        cast(col('Brand'), StringType()).as_("Brand"),
        cast(col('Model'), StringType()).as_("Model"),
        cast(col('Color'), StringType()).as_("Color"),
        cast(col('Memory'), StringType()).as_("Memory")
    )
    
    product_dim_df = product_dim_df.groupBy(col('mobile_key'), col("Brand"), col("Model"), col("Color"), col("Memory")).count()
    product_dim_df = product_dim_df.with_column("isActive", lit('Y'))
    
    existing_product_dim_df = session.sql("select mobile_key, Brand, Model, Color, Memory from sales_dwh.consumption.product_dim")
    product_dim_df = product_dim_df.join(existing_product_dim_df, ["mobile_key", "Brand", "Model", "Color", "Memory"], join_type='leftanti')
    
    product_dim_df = product_dim_df.selectExpr("sales_dwh.consumption.product_dim_seq.nextval as product_id_pk", "mobile_key", "Brand", "Model", "Color", "Memory", "isActive") 
    
    insert_cnt = int(product_dim_df.count())
    if insert_cnt > 0:
        product_dim_df.write.save_as_table("sales_dwh.consumption.product_dim", mode="append")
        logging.info(f"✓ Product Dimension: {insert_cnt} rows inserted")
    else:
        logging.info("✓ Product Dimension: No new records to insert")

# Promo Code Dimension
def create_promocode_dim(all_sales_df, session) -> None:
    logging.info("Creating Promo Code Dimension...")
    
    # Check if promotion_code exists in source
    src_cols_map = {c.lower(): c for c in all_sales_df.columns}
    promo_src = src_cols_map.get("promotion_code")
    country_src = src_cols_map.get("country")
    region_src = src_cols_map.get("region")
    
    if country_src is None or region_src is None:
        logging.error("❌ Error in create_promocode_dim: country or region column not found in source")
        return
    
    # If promotion_code doesn't exist, create with 'NA'
    if promo_src is None:
        logging.warning("⚠ promotion_code column not found in source, using 'NA' as default")
        promo_code_df = all_sales_df.with_column("promotion_code", lit('NA'))
    else:
        promo_code_df = all_sales_df.with_column("promotion_code",
                                                 when(col(promo_src).isNull(), lit('NA'))
                                                 .otherwise(col(promo_src)))
    
    # Normalize columns
    promo_code_df = promo_code_df.select(
        col("promotion_code"),
        col(country_src).as_("country"),
        col(region_src).as_("region")
    )
    
    promo_code_dim_df = promo_code_df.groupBy(col("promotion_code"), col("country"), col("region")).count()
    promo_code_dim_df = promo_code_dim_df.with_column("isActive", lit('Y'))
    
    # Read existing and compare
    existing_promo_df = session.sql("select * from sales_dwh.consumption.promo_code_dim")
    existing_map = {c.lower(): c for c in existing_promo_df.columns}
    existing_promo_col = existing_map.get("promotion_code") or existing_map.get("promo_code")
    existing_country_col = existing_map.get("country")
    existing_region_col = existing_map.get("region")
    
    if existing_promo_col is None or existing_country_col is None or existing_region_col is None:
        logging.warning("⚠ promo_code_dim table missing expected columns. Recreating table...")
        
        promo_code_dim_df = promo_code_dim_df.with_column("promo_code_id_pk", expr("sales_dwh.consumption.promo_code_dim_seq.nextval"))
        promo_code_dim_df = promo_code_dim_df.select("promo_code_id_pk", "promotion_code", "country", "region", "isActive")
        
        promo_code_dim_df.write.save_as_table("sales_dwh.consumption.promo_code_dim", mode="overwrite")
        logging.info(f"✓ Promo Code Dimension: Recreated table with {promo_code_dim_df.count()} rows")
        return
    
    # Explicit join condition
    join_cond = (
        promo_code_dim_df.col("promotion_code") == existing_promo_df.col(existing_promo_col)
    ) & (
        promo_code_dim_df.col("country") == existing_promo_df.col(existing_country_col)
    ) & (
        promo_code_dim_df.col("region") == existing_promo_df.col(existing_region_col)
    )
    promo_code_dim_df = promo_code_dim_df.join(existing_promo_df, join_cond, join_type='leftanti')
    
    promo_code_dim_df = promo_code_dim_df.with_column("promo_code_id_pk", expr("sales_dwh.consumption.promo_code_dim_seq.nextval"))
    promo_code_dim_df = promo_code_dim_df.select("promo_code_id_pk", "promotion_code", "country", "region", "isActive")
    
    insert_cnt = int(promo_code_dim_df.count())
    if insert_cnt > 0:
        promo_code_dim_df.write.save_as_table("sales_dwh.consumption.promo_code_dim", mode="append")
        logging.info(f"✓ Promo Code Dimension: {insert_cnt} rows inserted")
    else:
        logging.info("✓ Promo Code Dimension: No new records to insert")
    
# Customer Dimension
def create_customer_dim(all_sales_df, session) -> None:
    logging.info("Creating Customer Dimension...")
    
    # Map source columns to handle case sensitivity
    src_cols_map = {c.lower(): c for c in all_sales_df.columns}
    country_src = src_cols_map.get("country")
    region_src = src_cols_map.get("region")
    customer_name_src = src_cols_map.get("customer_name")
    contact_no_src = src_cols_map.get("contact_no")
    shipping_address_src = src_cols_map.get("shipping_address")
    
    if not all([country_src, region_src, customer_name_src, contact_no_src, shipping_address_src]):
        logging.error("❌ Error in create_customer_dim: one or more required columns not found in source")
        return
    
    # Group by actual source column names
    customer_dim_df = all_sales_df.groupBy(
        col(country_src),
        col(region_src),
        col(customer_name_src),
        col(contact_no_src),
        col(shipping_address_src)
    ).count()
    
    customer_dim_df = customer_dim_df.with_column("isActive", lit('Y'))
    
    # Normalize to lowercase for consistency
    customer_dim_df = customer_dim_df.select(
        col(customer_name_src).as_("customer_name"),
        col(contact_no_src).as_("contact_no"),
        col(shipping_address_src).as_("shipping_address"),
        col(country_src).as_("country"),
        col(region_src).as_("region"),
        col("isActive")
    )
    
    existing_customer_dim_df = session.sql("select * from sales_dwh.consumption.customer_dim")
    existing_cols_map = {c.lower(): c for c in existing_customer_dim_df.columns}
    
    target_customer_name = existing_cols_map.get("customer_name")
    target_contact_no = existing_cols_map.get("contact_no") or existing_cols_map.get("conctact_no")
    target_shipping_address = existing_cols_map.get("shipping_address")
    target_country = existing_cols_map.get("country")
    target_region = existing_cols_map.get("region")
    
    if not all([target_customer_name, target_contact_no, target_shipping_address, target_country, target_region]):
        logging.error("❌ Error in create_customer_dim: Target table missing required columns")
        return

    join_cond = (
        (customer_dim_df.col("customer_name") == existing_customer_dim_df.col(target_customer_name)) &
        (customer_dim_df.col("contact_no") == existing_customer_dim_df.col(target_contact_no)) &
        (customer_dim_df.col("shipping_address") == existing_customer_dim_df.col(target_shipping_address)) &
        (customer_dim_df.col("country") == existing_customer_dim_df.col(target_country)) &
        (customer_dim_df.col("region") == existing_customer_dim_df.col(target_region))
    )
    customer_dim_df = customer_dim_df.join(existing_customer_dim_df, join_cond, join_type='leftanti')
    
    customer_dim_df = customer_dim_df.selectExpr("sales_dwh.consumption.customer_dim_seq.nextval as customer_id_pk", "customer_name", "contact_no", "shipping_address", "country", "region", "isActive") 
    
    insert_cnt = int(customer_dim_df.count())
    if insert_cnt > 0:
        customer_dim_df.write.save_as_table("sales_dwh.consumption.customer_dim", mode="append")
        logging.info(f"✓ Customer Dimension: {insert_cnt} rows inserted")
    else:
        logging.info("✓ Customer Dimension: No new records to insert")

# Payment Dimension

def create_payment_dim(all_sales_df, session) -> None:
    logging.info("Creating Payment Dimension...")
    
    payment_dim_df = all_sales_df.groupBy(col("COUNTRY"), col("REGION"), col("payment_method"), col("payment_provider")).count()
    payment_dim_df = payment_dim_df.with_column("isActive", lit('Y'))
    
    existing_payment_dim_df = session.sql("select payment_method, payment_provider, country, region from sales_dwh.consumption.payment_dim")
    payment_dim_df = payment_dim_df.join(existing_payment_dim_df, ["payment_method", "payment_provider", "country", "region"], join_type='leftanti')
    
    payment_dim_df = payment_dim_df.selectExpr("sales_dwh.consumption.payment_dim_seq.nextval as payment_id_pk", "payment_method", "payment_provider", "country", "region", "isActive") 
    
    insert_cnt = int(payment_dim_df.count())
    if insert_cnt > 0:
        payment_dim_df.write.save_as_table("sales_dwh.consumption.payment_dim", mode="append")
        logging.info(f"✓ Payment Dimension: {insert_cnt} rows inserted")
    else:
        logging.info("✓ Payment Dimension: No new records to insert")

# Date Dimension
def create_date_dim(all_sales_df, session) -> None:
    logging.info("Creating Date Dimension...")
    
    try:
        # Get min and max dates
        min_max_df = all_sales_df.select(
            min("order_dt").alias("min_date"),
            max("order_dt").alias("max_date")
        ).collect()[0]
        
        start_date = min_max_df['MIN_DATE']
        end_date = min_max_df['MAX_DATE']
        
        logging.info(f"Date range: {start_date} to {end_date}")
        
        # Calculate the number of days (Python calculation)
        from datetime import datetime
        start_dt = datetime.strptime(str(start_date), '%Y-%m-%d')
        end_dt = datetime.strptime(str(end_date), '%Y-%m-%d')
        num_days = (end_dt - start_dt).days + 1
        
        logging.info(f"Generating {num_days} dates...")
        
        # Use SQL to generate date dimension with CONSTANT rowcount
        date_gen_sql = f"""
        WITH date_spine AS (
            SELECT 
                DATEADD(day, SEQ4(), '{start_date}'::DATE) AS order_dt
            FROM TABLE(GENERATOR(ROWCOUNT => {num_days}))
        ),
        date_attributes AS (
            SELECT 
                order_dt,
                ROW_NUMBER() OVER (ORDER BY order_dt) AS day_counter,
                YEAR(order_dt) AS order_year,
                MONTH(order_dt) AS order_month,
                QUARTER(order_dt) AS order_quarter,
                DAY(order_dt) AS order_day,
                DAYOFWEEK(order_dt) AS order_dayofweek,
                DAYNAME(order_dt) AS order_dayname,
                DAY(order_dt) AS order_dayofmonth,
                CASE 
                    WHEN DAYOFWEEK(order_dt) IN (0, 6) THEN 'Weekend'
                    ELSE 'Weekday'
                END AS order_weekday
            FROM date_spine
        )
        SELECT * FROM date_attributes
        WHERE order_dt NOT IN (SELECT DISTINCT order_dt FROM sales_dwh.consumption.date_dim)
        """
        
        # Get new dates to insert
        new_dates_df = session.sql(date_gen_sql)
        insert_cnt = new_dates_df.count()
        
        if insert_cnt > 0:
            # Include date_id_pk generated from sequence
            new_dates_df = new_dates_df.selectExpr(
                "sales_dwh.consumption.date_dim_seq.nextval as date_id_pk",
                "order_dt",
                "day_counter",
                "order_year",
                "order_month",
                "order_quarter",
                "order_dayofweek",
                "order_dayname",
                "order_dayofmonth",
                "order_weekday"
            )
            
            new_dates_df.write.save_as_table("sales_dwh.consumption.date_dim", mode="append")
            logging.info(f"✓ Date Dimension: {insert_cnt} rows inserted")
        else:
            logging.info("✓ Date Dimension: No new dates to insert")
            
    except Exception as e:
        logging.error(f"❌ Error in create_date_dim: {str(e)}")
        raise

def main():
    try:
        session = get_snowpark_session()
        logging.info("=" * 60)
        logging.info("Starting Curated → Consumption transformation...")
        logging.info("=" * 60)
        
        # Load curated data
        logging.info("Loading curated sales data...")
        in_sales_df = session.sql("select * from sales_dwh.curated.in_sales_order")
        us_sales_df = session.sql("select * from sales_dwh.curated.us_sales_order")
        fr_sales_df = session.sql("select * from sales_dwh.curated.fr_sales_order")
        
        all_sales_df = in_sales_df.union(us_sales_df).union(fr_sales_df)
        total_rows = all_sales_df.count()
        logging.info(f"Total curated records loaded: {total_rows}")
        logging.info("=" * 60)
        
        # Create all dimension tables
        create_date_dim(all_sales_df, session)
        create_region_dim(all_sales_df, session)
        create_product_dim(all_sales_df, session)
        create_promocode_dim(all_sales_df, session)
        create_customer_dim(all_sales_df, session)
        create_payment_dim(all_sales_df, session)
        
        logging.info("=" * 60)
        logging.info("Creating Sales Fact table...")
        
        # Load dimension tables
        date_dim_df = session.sql("select * from sales_dwh.consumption.date_dim")
        customer_dim_df = session.sql("select * from sales_dwh.consumption.customer_dim")
        payment_dim_df = session.sql("select * from sales_dwh.consumption.payment_dim")
        product_dim_df = session.sql("select * from sales_dwh.consumption.product_dim")
        promo_code_dim_df = session.sql("select * from sales_dwh.consumption.promo_code_dim")
        region_dim_df = session.sql("select * from sales_dwh.consumption.region_dim")
        
        def get_col(df, col_name):
            col_map = {c.lower(): c for c in df.columns}
            resolved_col = col_map.get(col_name.lower())
            
            # Handle potential column name mismatches
            if not resolved_col and col_name.lower() == 'promotion_code':
                resolved_col = col_map.get('promo_code')
            
            if not resolved_col:
                raise ValueError(f"Column '{col_name}' not found. Available: {list(col_map.values())}")
            return df.col(resolved_col)
        
        # Join sales with dimensions
        all_sales_df = all_sales_df.with_column("promotion_code", expr("case when promotion_code is null then 'NA' else promotion_code end"))
        
        all_sales_df = all_sales_df.join(date_dim_df, all_sales_df.col("order_dt") == get_col(date_dim_df, "order_dt"), join_type='inner', rsuffix='_date')
        
        all_sales_df = all_sales_df.join(customer_dim_df, 
                                         (all_sales_df.col("customer_name") == get_col(customer_dim_df, "customer_name")) &
                                         (all_sales_df.col("region") == get_col(customer_dim_df, "region")) &
                                         (all_sales_df.col("country") == get_col(customer_dim_df, "country")), 
                                         join_type='inner', rsuffix='_cust')
                                         
        all_sales_df = all_sales_df.join(payment_dim_df, 
                                         (all_sales_df.col("payment_method") == get_col(payment_dim_df, "payment_method")) &
                                         (all_sales_df.col("payment_provider") == get_col(payment_dim_df, "payment_provider")) &
                                         (all_sales_df.col("country") == get_col(payment_dim_df, "country")) &
                                         (all_sales_df.col("region") == get_col(payment_dim_df, "region")), 
                                         join_type='inner', rsuffix='_pay')
                                         
        all_sales_df = all_sales_df.join(product_dim_df, all_sales_df.col("mobile_key") == get_col(product_dim_df, "mobile_key"), join_type='inner', rsuffix='_prod')
        
        all_sales_df = all_sales_df.join(promo_code_dim_df, 
                                         (all_sales_df.col("promotion_code") == get_col(promo_code_dim_df, "promotion_code")) &
                                         (all_sales_df.col("country") == get_col(promo_code_dim_df, "country")) &
                                         (all_sales_df.col("region") == get_col(promo_code_dim_df, "region")), 
                                         join_type='inner', rsuffix='_promo')
                                         
        all_sales_df = all_sales_df.join(region_dim_df, 
                                         (all_sales_df.col("country") == get_col(region_dim_df, "country")) &
                                         (all_sales_df.col("region") == get_col(region_dim_df, "region")), 
                                         join_type='inner', rsuffix='_reg')
        
        session.sql("CREATE SEQUENCE IF NOT EXISTS sales_dwh.consumption.sales_fact_seq").collect()
        
        all_sales_df = all_sales_df.selectExpr(
            "sales_dwh.consumption.sales_fact_seq.nextval as order_id_pk",
            "order_id as order_code",
            "date_id_pk as date_id_fk",
            "region_id_pk as region_id_fk",
            "customer_id_pk as customer_id_fk",
            "payment_id_pk as payment_id_fk",
            "product_id_pk as product_id_fk",
            "promo_code_id_pk as promo_code_id_fk",
            "order_quantity",
            "local_total_order_amt",
            "local_tax_amt",
            "exchange_rate",
            "usd_total_order_amt",
            "usd_tax_amt"
        )
        
        fact_count = all_sales_df.count()
        all_sales_df.write.save_as_table("sales_dwh.consumption.sales_fact", mode="append")
        
        logging.info(f"✓ Sales Fact: {fact_count} rows inserted")
        logging.info("=" * 60)
        logging.info("✓ Transformation complete! Consumption layer summary:")
        logging.info(f"  Total fact records: {fact_count}")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")
        raise
    finally:
        session.close()
        logging.info("Session closed")

if __name__ == '__main__':
    main()
