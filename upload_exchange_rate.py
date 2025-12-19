from snowflake.snowpark import Session
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

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

def main():
    local_file = '/Users/kshitijkharche/Desktop/snowpark-e2e/end2end-sample-data/exchange-rate-data.csv'
    
    if not os.path.exists(local_file):
        logging.error(f"File not found: {local_file}")
        return
    
    session = get_snowpark_session()
    
    try:
        logging.info("Uploading exchange rate file to Snowflake stage...")
        put_result = session.file.put(
            local_file,
            '@sales_dwh.source.my_internal_stg/exchange',
            auto_compress=False,
            overwrite=True,
            parallel=10
        )
        logging.info(f"✓ Upload status: {put_result[0].status}")
        logging.info("✓ Exchange rate file uploaded successfully!")
    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")
    finally:
        session.close()
        logging.info("Session closed")

if __name__ == '__main__':
    main()
