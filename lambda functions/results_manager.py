import json
import boto3
import os

def lambda_handler(event, context):
    s3 = boto3.resource('s3')

    if event:
        file_object = event['Records'][0]
        key = str(file_object['s3']['object']['key'])
        bucket = str(file_object['s3']['bucket']['name'])
        results_bucket = os.environ['RESULTS_BUCKET']

        if 'average_waiting_minutes_last_week' in key:
            s3.Object(results_bucket,'average_waiting_minutes_last_week').copy_from(CopySource=bucket + "/" + key)
        elif 'average_waiting_minutes' in key:
            s3.Object(results_bucket,'average_waiting_time_minutes.csv').copy_from(CopySource=bucket + "/" + key)
        elif 'average_waiting_over_the_day' in key:
            s3.Object(results_bucket,'average_waiting_over_the_day.csv').copy_from(CopySource=bucket + "/" + key)
        elif 'longest_waiting_time' in key:
            s3.Object(results_bucket,'longest_waiting_time.csv').copy_from(CopySource=bucket + "/" + key)
        elif 'average_waiting_by_location_by_routes' in key:
            s3.Object(results_bucket,'average_waiting_by_location_by_routes.csv').copy_from(CopySource=bucket + "/" + key)
        elif 'average_waiting_by_location' in key:
            s3.Object(results_bucket,'average_waiting_by_location.csv').copy_from(CopySource=bucket + "/" + key)


        s3.Object(bucket,key).delete()
        s3.Object(bucket,key + ".metadata").delete()

    return {
        'statusCode': 200,
    }
