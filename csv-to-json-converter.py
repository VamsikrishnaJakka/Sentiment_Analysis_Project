import json
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the S3 bucket and key from the event
    s3_bucket = 'sentimental-analysis-internship-project'
    s3_key = 'csv-inputs/amazon.csv'

    
    # Read the CSV file from S3
    csv_obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    csv_data = csv_obj['Body'].read().decode('utf-8').splitlines()
    
    # Convert CSV to JSON
    header = csv_data[0].split(',')
    result = []
    for row in csv_data[1:]:
        row_values = row.split(',')
        if len(row_values) != len(header):
            continue
        row_dict = {}
        for i, value in enumerate(row_values):
            row_dict[header[i]] = value
        result.append(row_dict)

    json_data = json.dumps(result)
    
    # Upload the JSON data to a different S3 bucket
    s3.put_object(Bucket='sentimental-analysis-internship-project', Key='json-outputs/{}.json'.format(s3_key.split('/')[-1].split('.')[0]), Body=json_data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('CSV to JSON conversion and upload complete')
    }
