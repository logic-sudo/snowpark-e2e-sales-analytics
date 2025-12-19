import sys
import logging
from snowflake.snowpark import Session

logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    datefmt='%I:%M:%S'
)

def get_snowpark_session() -> Session:
    """Create Snowflake session"""
    connection_parameters = {
        "ACCOUNT": "QNNERMQ-RZ07987",
        "USER": "snowpark_user",
        "PASSWORD": "Test@12$4",
        "ROLE": "SYSADMIN",
        "DATABASE": "SALES_DWH",
        "SCHEMA": "SOURCE",
        "WAREHOUSE": "SNOWPARK_ETL_WH"
    }
    return Session.builder.configs(connection_parameters).create()

def ingest_in_sales(session) -> None:
    """Load India sales data from CSV files"""
    logging.info("Loading India sales data (CSV)...")
    
    result = session.sql("""
        COPY INTO IN_SALES_ORDER 
        FROM (
            SELECT
                IN_SALES_ORDER_SEQ.NEXTVAL,
                t.$1::text,
                t.$2::text,
                t.$3::text,
                t.$4::number,
                t.$5::number,
                t.$6::number,
                t.$7::text,
                t.$8::number(10,2),
                t.$9::number(10,2),
                t.$10::date,
                t.$11::text,
                t.$12::text,
                t.$13::text,
                t.$14::text,
                t.$15::text,
                t.$16::text,
                metadata$filename,
                metadata$file_row_number,
                metadata$file_last_modified
            FROM @MY_INTERNAL_STG/sales/source=IN/format=csv/
            (file_format => 'SALES_DWH.COMMON.MY_CSV_FORMAT') t
        ) 
        ON_ERROR = 'CONTINUE'
    """).collect()
    
    logging.info(f"✓ India sales loaded: {result}")

def ingest_us_sales(session) -> None:
    """Load USA sales data from Parquet files"""
    logging.info("Loading USA sales data (Parquet)...")
    
    result = session.sql("""
        COPY INTO US_SALES_ORDER
        FROM (
            SELECT
                US_SALES_ORDER_SEQ.NEXTVAL,
                $1:"Order ID"::text,
                $1:"Customer Name"::text,
                $1:"Mobile Model"::text,
                TO_NUMBER($1:"Quantity"),
                TO_NUMBER($1:"Price per Unit"),
                TO_DECIMAL($1:"Total Price"),
                $1:"Promotion Code"::text,
                $1:"Order Amount"::number(10,2),
                TO_DECIMAL($1:"Tax"),
                $1:"Order Date"::date,
                $1:"Payment Status"::text,
                $1:"Shipping Status"::text,
                $1:"Payment Method"::text,
                $1:"Payment Provider"::text,
                $1:"Phone"::text,
                $1:"Delivery Address"::text,
                metadata$filename,
                metadata$file_row_number,
                metadata$file_last_modified
            FROM @MY_INTERNAL_STG/sales/source=US/format=parquet/
            (file_format => SALES_DWH.COMMON.MY_PARQUET_FORMAT)
        ) 
        ON_ERROR = 'CONTINUE'
    """).collect()
    
    logging.info(f"✓ USA sales loaded: {result}")

def ingest_fr_sales(session) -> None:
    """Load France sales data from JSON files"""
    logging.info("Loading France sales data (JSON)...")
    
    result = session.sql("""
        COPY INTO FR_SALES_ORDER
        FROM (
            SELECT
                FR_SALES_ORDER_SEQ.NEXTVAL,
                $1:"Order ID"::text,
                $1:"Customer Name"::text,
                $1:"Mobile Model"::text,
                TO_NUMBER($1:"Quantity"),
                TO_NUMBER($1:"Price per Unit"),
                TO_DECIMAL($1:"Total Price"),
                $1:"Promotion Code"::text,
                $1:"Order Amount"::number(10,2),
                TO_DECIMAL($1:"Tax"),
                $1:"Order Date"::date,
                $1:"Payment Status"::text,
                $1:"Shipping Status"::text,
                $1:"Payment Method"::text,
                $1:"Payment Provider"::text,
                $1:"Phone"::text,
                $1:"Delivery Address"::text,
                metadata$filename,
                metadata$file_row_number,
                metadata$file_last_modified
            FROM @MY_INTERNAL_STG/sales/source=FR/format=json/
            (file_format => SALES_DWH.COMMON.MY_JSON_FORMAT)
        ) 
        ON_ERROR = 'CONTINUE'
    """).collect()
    
    logging.info(f"✓ France sales loaded: {result}")

def main():
    logging.info("=" * 60)
    logging.info("Starting sales data ingestion process...")
    logging.info("=" * 60)
    
    session = get_snowpark_session()
    
    try:
        # Verify context
        context = session.sql("SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()").collect()
        logging.info(f"Session context: {context[0]}")
        
        # Verify tables exist
        tables = session.sql("SHOW TABLES IN SCHEMA SOURCE").collect()
        logging.info(f"Tables found: {len(tables)}")
        for table in tables:
            logging.info(f"  - {table['name']}")
        
        if len(tables) == 0:
            raise Exception("No tables found! Check permissions.")
        
        # Load all regions
        ingest_in_sales(session)
        ingest_us_sales(session)
        ingest_fr_sales(session)
        
        # Verify data loaded
        counts = session.sql("""
            SELECT 'India' as region, COUNT(*) as cnt FROM IN_SALES_ORDER
            UNION ALL
            SELECT 'USA', COUNT(*) FROM US_SALES_ORDER
            UNION ALL
            SELECT 'France', COUNT(*) FROM FR_SALES_ORDER
        """).collect()
        
        logging.info("=" * 60)
        logging.info("✓ Data load summary:")
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
