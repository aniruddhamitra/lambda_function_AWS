import json
import boto3
import csv

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # Get the bucket name and file key from the event
    bucket_name = event['bucket_name']
    file_key = event['file_key']
    
    # Get the CSV file from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    content = response['Body'].read().decode('utf-8').splitlines()
    
    # Read the CSV file
    csv_reader = csv.reader(content)
    csv_data = [row for row in csv_reader]
    
    # Print the CSV data (for debugging purposes)
    print(csv_data)
    
    # Return the CSV data as a JSON response
    # Database connection settings
    db_host = 'your-db-host'
    db_user = 'your-db-username'
    db_password = 'your-db-password'
    db_name = 'your-db-name'
    db_table = 'your-db-table'
    
    # Connect to the database
    connection = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )
    
    try:
        with connection.cursor() as cursor:
            # Insert CSV data into the database table
            for row in csv_data:
                sql = f"INSERT INTO {db_table} (column1, column2, column3) VALUES (%s, %s, %s)"
                cursor.execute(sql, row)
        
        # Commit the transaction
        connection.commit()
    finally:
        # Close the database connection
        connection.close()
    
    # Return a success response
    return {
        'statusCode': 200,
        'body': json.dumps('CSV data inserted into the database successfully')
    }
    