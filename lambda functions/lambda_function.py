import trip_updates_feed as tuf
import stops
import cassandra_session_init as csi
import time


def lambda_handler(event, context):

    current_time = int(time.time())
    cassandra_session = csi.cassandra_session_init()
    feed = tuf.update_feed()
    stop_feed = stops.get_stops(feed,current_time)
    stops.insert_stops(stop_feed, current_time, cassandra_session)

    return {
        'statusCode': 200
    }
