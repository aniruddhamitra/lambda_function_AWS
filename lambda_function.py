import boto3
import csv
import pymysql  # or psycopg2 for PostgreSQL
import os
import io
import zipfile

# Initialize boto3 clients
s3_client = boto3.client('s3')

# Database connection parameters from environment variables
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']

def lambda_handler(event, context):
    # 1. Get S3 bucket and object key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key    = event['Records'][0]['s3']['object']['key']
    
    # 2. Download CSV file content
    response = s3_client.get_object(Bucket=bucket, Key=key)
    csv_content = response['Body'].read().decode('utf-8')
    csv_reader = csv.reader(io.StringIO(csv_content))

    # 3. Connect to the database
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    
    try:
        with connection.cursor() as cursor:
            # Example SQL: adjust to your schema
            for row in csv_reader:
                # Assuming CSV columns: col1, col2, col3
                sql = "INSERT INTO your_table (col1, col2, col3) VALUES (%s, %s, %s)"
                cursor.execute(sql, row)
            connection.commit()
    finally:
        connection.close()
    
    # 4. Zip the processed CSV file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(os.path.basename(key), csv_content)
    zip_buffer.seek(0)
    
    # 5. Upload zipped file to S3 (with .zip extension)
    zip_key = key.rsplit('.', 1)[0] + '.zip'
    s3_client.upload_fileobj(zip_buffer, bucket, zip_key)
    
    # 6. Delete the original CSV file (optional, if you want to remove it)
    s3_client.delete_object(Bucket=bucket, Key=key)
    
    return {
        'statusCode': 200,
        'body': f'Processed and zipped {key} to {zip_key}'
    }