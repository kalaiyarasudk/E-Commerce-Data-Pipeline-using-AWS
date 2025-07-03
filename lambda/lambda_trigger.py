import json
import urllib.parse
import boto3

# Step Functions client
sfn = boto3.client('stepfunctions')

STEP_FUNCTION_ARN = 'arn:aws:states:eu-north-1:##'

def lambda_handler(event, context):
    for record in event.get('Records', []):
        try:
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')

            print(f"New file uploaded: {key}")

            if not key.endswith('.csv'):
                print(f"Skipping non-CSV file: {key}")
                continue
            if (
                key.startswith('ecommerce-data-raw/customers/') or
                key.startswith('ecommerce-data-raw/products/') or
                key.startswith('ecommerce-data-raw/orders/')
            ):
                dataset_type = key.split('/')[1] 
                print(f"Processing dataset: {dataset_type}")
                input_payload = {
                    'bucket': bucket,
                    'key': key,
                    'dataset': dataset_type
                }

                # Start the Step Function execution
                response = sfn.start_execution(
                    stateMachineArn=STEP_FUNCTION_ARN,
                    input=json.dumps(input_payload)
                )

                print(f"Step Function started: {response['executionArn']}")
            else:
                print(f"File in unmonitored path: {key}")

        except Exception as e:
            print(f"Error: {str(e)}")
            raise

    return {
        'statusCode': 200,
        'message': 'Lambda execution complete.'
    }
