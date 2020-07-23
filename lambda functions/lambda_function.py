import trip_updates_feed as tuf
import stops
#import cassandra_session_init as csi
import time
from datetime import datetime, timezone
import queries
import csv
import os
import boto3

def lambda_handler(event, context):

    current_time = int(time.time())
    #cassandra_session = csi.cassandra_session_init()
    feed = tuf.update_feed()
    stop_feed = stops.get_stops(feed, current_time)
    s3 = boto3.resource('s3')
    athena = boto3.client('athena')
    stops.insert_stops(stop_feed, current_time, s3)
    
    current_time = current_time + 7200
    date = time.strftime('%Y-%m-%d', time.localtime(current_time))
    hour = time.strftime('%H', time.localtime(current_time))
    bucket = os.environ['BUCKET_NAME']
    table = os.environ['ATHENA_TABLE']
    
    queryStart = athena.start_query_execution(
        QueryString='ALTER TABLE ' + table + ' ADD PARTITION (date = \'' + date + '\',hour = ' + hour + ' ) LOCATION \'s3://' + bucket + '/date=' + date + '/hour=' + hour + '/\'',
        QueryExecutionContext={
            'Database': os.environ['ATHENA_DB']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + bucket + '/results/',
        }
    )
    
    
    queries.execute_queries(athena)
    
    


    return {
        'statusCode': 200
    }
