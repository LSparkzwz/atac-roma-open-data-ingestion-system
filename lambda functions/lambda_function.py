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
    stops.insert_stops(stop_feed, current_time, s3, athena)

    queries.execute_queries(athena)




    return {
        'statusCode': 200
    }
