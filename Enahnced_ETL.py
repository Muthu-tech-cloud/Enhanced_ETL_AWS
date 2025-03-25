import os
import boto3
import pandas as pd
import logging
import json
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, text
from credentials import HOST,PORT,DATABASE,PASSWORD,USER


# configure logging
LOG_FILE = r"log_file.txt"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(message)s", filemode="w")
def log_message(message):
    """Log a message."""
    logging.info(message)

# AWS S3 Configuration
BUCKET_NAME = "muthu-etl-project-bucket"
LOCAL_FOLDER = r"C:\Users\uie76632\Downloads\source"  # Source folder for upload
DOWNLOAD_FOLDER = r"D:\Files"  # Folder for downloads

# Ensure local download folder exists
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

# Create an S3 client
s3_client = boto3.client("s3")

def upload_files_to_s3(local_folder, bucket_name, s3_client):
    """Upload all files from a local folder to an S3 bucket."""
    for file_name in os.listdir(local_folder):
        file_path = os.path.join(local_folder, file_name)
        if os.path.isfile(file_path):
            try:
                s3_client.upload_file(file_path, bucket_name, file_name)
                log_message(f"Uploaded: {file_name} to {bucket_name}/{file_name}")
            except Exception as exception:
                log_message(f"Upload failed for {file_name}: {exception}")

upload_files_to_s3(LOCAL_FOLDER, BUCKET_NAME, s3_client)

def download_files_from_s3(bucket_name, download_folder, s3_client):
    """Download all files from an S3 bucket to a local folder."""
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if "Contents" in response:
        for obj in response["Contents"]:
            file_name = obj["Key"]
            local_file_path = os.path.join(download_folder, file_name)
            try:
                s3_client.download_file(bucket_name, file_name, local_file_path)
                log_message(f"Downloaded: {file_name} to {local_file_path}")
            except Exception as e:
                log_message(f"Download failed for {file_name}: {e}")
    else:
        log_message("No files found in the bucket.")

download_files_from_s3(BUCKET_NAME, DOWNLOAD_FOLDER, s3_client)

def extract_csv(file_path):
    """Extract data from a CSV file into a DataFrame."""
    log_message(f"Extracting: {file_path}")
    return pd.read_csv(file_path)

def extract_json(file_path):
    """Extract data from a JSON file into a DataFrame."""
    log_message(f"Extracting: {file_path}")
    with open(file_path, 'r') as file:
        try:
            data = json.load(file)  # Try to load a single valid JSON structure
            if isinstance(data, list):  # If JSON is a list of dictionaries
                return pd.DataFrame(data)
            else:
                return pd.DataFrame([data])  # Convert a single dictionary into a DataFrame
        except json.JSONDecodeError:
            # Handle a newline-separated JSON (JSONL)
            file.seek(0)  # Reset file read position
            data = [json.loads(line) for line in file]  # Read line by line
            return pd.DataFrame(data)

def extract_xml(file_path):
    """Extract data from an XML file into a DataFrame."""
    log_message(f"Extracting: {file_path}")
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    columns = [elem.tag for elem in root[0]] if len(root) > 0 else []
    for record in root:
        data.append([elem.text for elem in record])
    return pd.DataFrame(data, columns=columns)

def master_extract(file_path):
    """Master function to extract data based on file type and combine into a single DataFrame."""
    log_message("Starting extraction")
    extracted_data = []
    for file_name in os.listdir(file_path):
        full_path = os.path.join(file_path, file_name)
        if isinstance(full_path, str):
            if full_path.endswith('.csv'):
                extracted_data.append(extract_csv(full_path))
            elif full_path.endswith('.json'):
                extracted_data.append(extract_json(full_path))
            elif full_path.endswith('.xml'):
                extracted_data.append(extract_xml(full_path))
            else:
                log_message(f"Unsupported file type: {full_path}")
        else:
            log_message(f"Invalid file path type: {full_path}")

    log_message("Extraction completed")
    return pd.concat(extracted_data, ignore_index=True) if extracted_data else pd.DataFrame()

def transform_data(df):
    """Transform data: Convert heights from inches to meters and weights from pounds to kilograms."""
    log_message("Starting transformation")
    if 'Height(in)' in df.columns:
        df['Height(m)'] = df['Height(in)'].astype(float) * 0.0254
    if 'Weight(lb)' in df.columns:
        df['Weight(kg)'] = df['Weight(lb)'].astype(float) * 0.453592
    log_message("Transformation completed")
    return df

def load_data(df, output_file):
    """Load the transformed data into a CSV file."""
    log_message(f"Saving to: {output_file}")
    df.to_csv(output_file, index=False)
    log_message("Data saved as .csv file")
    return output_file

combined_df = master_extract(DOWNLOAD_FOLDER)
transformed_df = transform_data(combined_df)
csv_file = load_data(transformed_df, 'transformed_data.csv')
S3_KEY = f"processed-data/{csv_file}"

# Upload processed data to S3
s3_client.upload_file(csv_file, BUCKET_NAME, S3_KEY)
log_message(f"File uploaded to S3: s3://{BUCKET_NAME}/{S3_KEY}")

# AWS RDS Connection Details
host = HOST
port = PORT
user = USER
password = PASSWORD
database = DATABASE

table_name = "persons"

# Create SQLAlchemy Connection String
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)

try:
    with engine.connect() as conn:
        log_message("Connected to AWS RDS MySQL successfully!")

        # Create Table if Not Exists
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            height FLOAT,
            weight FLOAT
        );
        """
        conn.execute(text(create_table_query))
        log_message(f"Table '{table_name}' checked/created successfully!")

        # Load CSV Data
        df = pd.read_csv(csv_file)

        # Upload DataFrame to MySQL Table
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        log_message("CSV Data Uploaded Successfully!")

except Exception as err:
    log_message(f"Error: {err}")
