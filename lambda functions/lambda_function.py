import trip_updates_feed as tuf
import cassandra_session_init as csi
import time


def lambda_handler(event, context):

    current_time = int(time.time())
    cassandra_session = csi.cassandra_session_init()
    feed = tuf.update_feed()
    stops = tuf.get_stops(feed,current_time)
    tuf.insert_stops(stops, current_time, cassandra_session)

    return {
        'statusCode': 200
    }
