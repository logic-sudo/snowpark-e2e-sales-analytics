import os
from snowflake.snowpark import Session
import sys
import logging

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s', 
                   datefmt='%I:%M:%S')

# snowpark session
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

def traverse_directory(directory, file_extension) -> list:
    local_file_path = []
    file_name = []
    partition_dir = []
    
    logging.info(f"Scanning directory: {directory} for {file_extension} files")
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root, file)
                file_name.append(file)
                # Get relative path
                rel_path = os.path.relpath(root, directory)
                partition_dir.append(rel_path if rel_path != '.' else '')
                local_file_path.append(file_path)
                logging.info(f"Found: {file} in {rel_path}")
    
    logging.info(f"Total {file_extension} files found: {len(file_name)}")
    return file_name, partition_dir, local_file_path

def upload_files(session, file_names, partition_dirs, local_paths, stage_location, file_type):
    """Upload files to Snowflake stage"""
    if not file_names:
        logging.warning(f"No {file_type} files to upload")
        return
    
    for idx, file_name in enumerate(file_names):
        try:
            target = f"{stage_location}/sales/{partition_dirs[idx]}" if partition_dirs[idx] else f"{stage_location}/sales"
            
            logging.info(f"Uploading {file_type}: {file_name} to {target}")
            
            put_result = session.file.put(
                local_paths[idx], 
                target, 
                auto_compress=False, 
                overwrite=True, 
                parallel=4
            )
            
            logging.info(f"✓ {file_name} => {put_result[0].status}")
            
        except Exception as e:
            logging.error(f"❌ Failed to upload {file_name}: {str(e)}")

def main():
    directory_path = '/Users/kshitijkharche/Desktop/snowpark-e2e/end2end-sample-data/sales'
    
    # Check if directory exists
    if not os.path.exists(directory_path):
        logging.error(f"Directory not found: {directory_path}")
        return
    
    # Get file lists
    csv_file_name, csv_partition_dir, csv_local_file_path = traverse_directory(directory_path, '.csv')
    parquet_file_name, parquet_partition_dir, parquet_local_file_path = traverse_directory(directory_path, '.parquet')
    json_file_name, json_partition_dir, json_local_file_path = traverse_directory(directory_path, '.json')
    
    stage_location = '@sales_dwh.source.my_internal_stg'
    
    # Create session ONCE and reuse it
    logging.info("Creating Snowflake session...")
    session = get_snowpark_session()
    
    try:
        # Upload all file types
        upload_files(session, csv_file_name, csv_partition_dir, csv_local_file_path, stage_location, "CSV")
        upload_files(session, parquet_file_name, parquet_partition_dir, parquet_local_file_path, stage_location, "PARQUET")
        upload_files(session, json_file_name, json_partition_dir, json_local_file_path, stage_location, "JSON")
        
        logging.info("=" * 60)
        logging.info("✓ All files uploaded successfully!")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"Error during upload: {str(e)}")
    finally:
        session.close()
        logging.info("Session closed")

if __name__ == '__main__':
    main()
