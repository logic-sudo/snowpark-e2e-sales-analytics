import sys
import logging

from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, lit, row_number, rank, year, month, quarter
from snowflake.snowpark import Window

# Initiate logging at info level
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    datefmt='%I:%M:%S'
)

# Snowpark session
def get_snowpark_session() -> Session:
    """Create Snowflake session"""
    connection_parameters = {
        "ACCOUNT": "QNNERMQ-RZ07987",
        "USER": "snowpark_user",
        "PASSWORD": "Test@12$4",
        "ROLE": "SYSADMIN",
        "DATABASE": "sales_dwh",
        "SCHEMA": "source",
        "WAREHOUSE": "SNOWPARK_ETL_WH"
    }
    return Session.builder.configs(connection_parameters).create()   

def filter_dataset(df, column_name, filter_criterion) -> DataFrame:
    """Filter dataset by column value"""
    return_df = df.filter(col(column_name) == filter_criterion)
    return return_df

def transform_india_sales(session):
    """Transform India sales from source to curated"""
    logging.info("=" * 60)
    logging.info("Starting India sales transformation (SOURCE → CURATED)...")
    logging.info("=" * 60)
    
    try:
        sales_df = session.sql("SELECT * FROM source.in_sales_order")
        logging.info(f"Source records loaded: {sales_df.count()}")

        paid_sales_df = filter_dataset(sales_df, 'PAYMENT_STATUS', 'Paid')
        shipped_sales_df = filter_dataset(paid_sales_df, 'SHIPPING_STATUS', 'Delivered')
        logging.info(f"After filtering (Paid & Delivered): {shipped_sales_df.count()}")

        country_sales_df = shipped_sales_df.with_column('Country', lit('India')).with_column('Region', lit('Asia'))

        # Join with exchange rate using USD2INR column
        forex_df = session.sql("SELECT DATE as EXCHANGE_DATE, USD2INR as EXCHANGE_RATE FROM sales_dwh.common.exchange_rate")
        sales_with_forex_df = country_sales_df.join(
            forex_df,
            country_sales_df['ORDER_DT'] == forex_df['EXCHANGE_DATE'],
            join_type='left'
        )

        # De-duplication
        unique_orders = sales_with_forex_df.with_column(
            'order_rank',
            rank().over(
                Window.partitionBy(col("ORDER_DT")).order_by(col('_METADATA_LAST_MODIFIED').desc())
            )
        ).filter(col("order_rank") == 1).select(
            col('SALES_ORDER_KEY').alias('unique_sales_order_key')
        )
        
        final_sales_df = unique_orders.join(
            sales_with_forex_df,
            unique_orders['unique_sales_order_key'] == sales_with_forex_df['SALES_ORDER_KEY'],
            join_type='inner'
        )
        
        # Select and transform columns
        final_sales_df = final_sales_df.select(
            col('SALES_ORDER_KEY'),
            col('ORDER_ID'),
            col('ORDER_DT'),
            col('CUSTOMER_NAME'),
            col('MOBILE_KEY'),
            col('Country'),
            col('Region'),
            col('ORDER_QUANTITY'),
            lit('INR').alias('LOCAL_CURRENCY'),
            col('UNIT_PRICE').alias('LOCAL_UNIT_PRICE'),
            col('PROMOTION_CODE'),
            col('FINAL_ORDER_AMOUNT').alias('LOCAL_TOTAL_ORDER_AMT'),
            col('TAX_AMOUNT').alias('LOCAL_TAX_AMT'),
            col('EXCHANGE_RATE'),
            (col('FINAL_ORDER_AMOUNT') / col('EXCHANGE_RATE')).alias('USD_TOTAL_ORDER_AMT'),
            (col('TAX_AMOUNT') / col('EXCHANGE_RATE')).alias('USD_TAX_AMT'),
            col('PAYMENT_STATUS'),
            col('SHIPPING_STATUS'),
            col('PAYMENT_METHOD'),
            col('PAYMENT_PROVIDER'),
            col('MOBILE').alias('CONTACT_NO'),
            col('SHIPPING_ADDRESS'),
            year(col('ORDER_DT')).alias('ORDER_YEAR'),
            month(col('ORDER_DT')).alias('ORDER_MONTH'),
            quarter(col('ORDER_DT')).alias('ORDER_QUARTER')
        )

        session.sql("TRUNCATE TABLE sales_dwh.curated.in_sales_order").collect()
        final_sales_df.write.save_as_table("sales_dwh.curated.in_sales_order", mode="append")
        
        final_count = session.sql("SELECT COUNT(*) as cnt FROM sales_dwh.curated.in_sales_order").collect()[0]['CNT']
        logging.info(f"✓ India sales transformed successfully: {final_count} rows")
        
    except Exception as e:
        logging.error(f"❌ Error transforming India sales: {str(e)}")
        raise

def transform_usa_sales(session):
    """Transform USA sales from source to curated"""
    logging.info("Starting USA sales transformation...")
    
    try:
        sales_df = session.sql("SELECT * FROM source.us_sales_order")
        
        paid_sales_df = filter_dataset(sales_df, 'PAYMENT_STATUS', 'Paid')
        shipped_sales_df = filter_dataset(paid_sales_df, 'SHIPPING_STATUS', 'Delivered')
        
        country_sales_df = shipped_sales_df.with_column('Country', lit('USA')).with_column('Region', lit('North America'))
        
        final_sales_df = country_sales_df.select(
            col('SALES_ORDER_KEY'),
            col('ORDER_ID'),
            col('ORDER_DT'),
            col('CUSTOMER_NAME'),
            col('MOBILE_KEY'),
            col('Country'),
            col('Region'),
            col('ORDER_QUANTITY'),
            lit('USD').alias('LOCAL_CURRENCY'),
            col('UNIT_PRICE').alias('LOCAL_UNIT_PRICE'),
            col('PROMOTION_CODE'),
            col('FINAL_ORDER_AMOUNT').alias('LOCAL_TOTAL_ORDER_AMT'),
            col('TAX_AMOUNT').alias('LOCAL_TAX_AMT'),
            lit(1.0000000).alias('EXCHANGE_RATE'),
            col('FINAL_ORDER_AMOUNT').alias('USD_TOTAL_ORDER_AMT'),
            col('TAX_AMOUNT').alias('USD_TAX_AMT'),
            col('PAYMENT_STATUS'),
            col('SHIPPING_STATUS'),
            col('PAYMENT_METHOD'),
            col('PAYMENT_PROVIDER'),
            col('PHONE').alias('CONTACT_NO'),
            col('SHIPPING_ADDRESS'),
            year(col('ORDER_DT')).alias('ORDER_YEAR'),
            month(col('ORDER_DT')).alias('ORDER_MONTH'),
            quarter(col('ORDER_DT')).alias('ORDER_QUARTER')
        )
        
        session.sql("TRUNCATE TABLE sales_dwh.curated.us_sales_order").collect()
        final_sales_df.write.save_as_table("sales_dwh.curated.us_sales_order", mode="append")
        
        final_count = session.sql("SELECT COUNT(*) as cnt FROM sales_dwh.curated.us_sales_order").collect()[0]['CNT']
        logging.info(f"✓ USA sales transformed: {final_count} rows")
        
    except Exception as e:
        logging.error(f"❌ Error transforming USA sales: {str(e)}")
        raise

def transform_france_sales(session):
    """Transform France sales from source to curated"""
    logging.info("Starting France sales transformation...")
    
    try:
        sales_df = session.sql("SELECT * FROM source.fr_sales_order")
        
        paid_sales_df = filter_dataset(sales_df, 'PAYMENT_STATUS', 'Paid')
        shipped_sales_df = filter_dataset(paid_sales_df, 'SHIPPING_STATUS', 'Delivered')
        
        country_sales_df = shipped_sales_df.with_column('Country', lit('France')).with_column('Region', lit('Europe'))
        
        # Join with exchange rate using USD2EU column
        forex_df = session.sql("SELECT DATE as EXCHANGE_DATE, USD2EU as EXCHANGE_RATE FROM sales_dwh.common.exchange_rate")
        sales_with_forex_df = country_sales_df.join(
            forex_df,
            country_sales_df['ORDER_DT'] == forex_df['EXCHANGE_DATE'],
            join_type='left'
        )
        
        final_sales_df = sales_with_forex_df.select(
            col('SALES_ORDER_KEY'),
            col('ORDER_ID'),
            col('ORDER_DT'),
            col('CUSTOMER_NAME'),
            col('MOBILE_KEY'),
            col('Country'),
            col('Region'),
            col('ORDER_QUANTITY'),
            lit('EUR').alias('LOCAL_CURRENCY'),
            col('UNIT_PRICE').alias('LOCAL_UNIT_PRICE'),
            col('PROMOTION_CODE'),
            col('FINAL_ORDER_AMOUNT').alias('LOCAL_TOTAL_ORDER_AMT'),
            col('TAX_AMOUNT').alias('LOCAL_TAX_AMT'),
            col('EXCHANGE_RATE'),
            (col('FINAL_ORDER_AMOUNT') / col('EXCHANGE_RATE')).alias('USD_TOTAL_ORDER_AMT'),
            (col('TAX_AMOUNT') / col('EXCHANGE_RATE')).alias('USD_TAX_AMT'),
            col('PAYMENT_STATUS'),
            col('SHIPPING_STATUS'),
            col('PAYMENT_METHOD'),
            col('PAYMENT_PROVIDER'),
            col('PHONE').alias('CONTACT_NO'),
            col('SHIPPING_ADDRESS'),
            year(col('ORDER_DT')).alias('ORDER_YEAR'),
            month(col('ORDER_DT')).alias('ORDER_MONTH'),
            quarter(col('ORDER_DT')).alias('ORDER_QUARTER')
        )
        
        session.sql("TRUNCATE TABLE sales_dwh.curated.fr_sales_order").collect()
        final_sales_df.write.save_as_table("sales_dwh.curated.fr_sales_order", mode="append")
        
        final_count = session.sql("SELECT COUNT(*) as cnt FROM sales_dwh.curated.fr_sales_order").collect()[0]['CNT']
        logging.info(f"✓ France sales transformed: {final_count} rows")
        
    except Exception as e:
        logging.error(f"❌ Error transforming France sales: {str(e)}")
        raise

def main():
    session = get_snowpark_session()
    
    try:
        transform_india_sales(session)
        transform_usa_sales(session)
        transform_france_sales(session)
        
        counts = session.sql("""
            SELECT 'India' as region, COUNT(*) as cnt FROM curated.in_sales_order
            UNION ALL
            SELECT 'USA', COUNT(*) FROM curated.us_sales_order
            UNION ALL
            SELECT 'France', COUNT(*) FROM curated.fr_sales_order
        """).collect()
        
        logging.info("=" * 60)
        logging.info("✓ Transformation complete! Curated layer summary:")
        for row in counts:
            logging.info(f"  {row['REGION']}: {row['CNT']} rows")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")
        raise
        
    finally:
        session.close()
        logging.info("Session closed")
    
if __name__ == '__main__':
    main()
