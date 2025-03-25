ETL Project: AWS S3, Pandas, and MySQL

📌 Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline using AWS S3, Pandas, and MySQL. The pipeline performs the following tasks:

Extract: Uploads and downloads files (CSV, JSON, XML) to/from an AWS S3 bucket.

Transform: Cleans the data and performs transformations.

Load: Stores the processed data into an AWS RDS MySQL database.

Logging: Logs all activities and uploads the log file to S3.

🚀 Technologies Used

Python (pandas, boto3, sqlalchemy, logging)

AWS S3 (storage)

AWS RDS MySQL (database)

SQLAlchemy (database connection)

Pandas (data transformation)

🔄 ETL Workflow

Upload Files to S3: Local files (CSV, JSON, XML) are uploaded to the S3 bucket.

Download Files from S3: The ETL script downloads the files from S3.

Extract Data: Data is extracted into Pandas DataFrames based on the file type.

Transform Data:

Converts heights from inches to meters.

Converts weights from pounds to kilograms.

Load Data into MySQL: The transformed data is inserted into an AWS RDS MySQL database.

Upload Log File to S3: The log file containing process details is uploaded to S3.

🛠️ Setup & Execution

1️⃣ Prerequisites

Ensure you have the following installed:

Python 3.x

AWS CLI configured with IAM permissions for S3

MySQL database running on AWS RDS

Required Python libraries:

pip install boto3 pandas sqlalchemy pymysql

2️⃣ Configure AWS & MySQL

Modify the script with your S3 bucket name and MySQL credentials:

BUCKET_NAME = "your-s3-bucket-name"
host = "your-mysql-rds-host"
user = "your-db-user"
password = "your-db-password"
database = "your-database-name"

3️⃣ Run the ETL Script

Execute the ETL script:

python Enahanced_ETL.py

4️⃣ Check the Processed Data

Verify the transformed data in your MySQL database:

SELECT * FROM persons;

5️⃣ Check Uploaded Log File in S3

Confirm the log file exists in the logs/ folder in your S3 bucket.

