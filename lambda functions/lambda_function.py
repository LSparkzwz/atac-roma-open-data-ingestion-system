import json
import trip_updates_feed as tuf
import cassandra_session_init as csi
import os
import string
from cassandra.cluster import Cluster, ConsistencyLevel, BatchStatement
from cassandra.query import BatchType

def lambda_handler(event, context):
    cassandra_session = csi.cassandra_session_init()
    cassandra_table = os.environ['CASSANDRA_TABLE']
    cassandra_row = os.environ['CASSANDRA_ROW']
    
    statement = "INSERT INTO " + cassandra_table + " " + cassandra_row + " VALUES (?,?,?,?,?)"
    insert_stop = cassandra_session.prepare(statement)
    batch = BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    #can only put 30 statements in a batch
    batch_counter = 0
    
    feed = tuf.update_feed()
    for entity in feed:
        trip_update = entity.trip_update
        trip = trip_update.trip
        
        trip_id = str(trip.trip_id)
        route_id = int(trip.route_id)
        
        #n stop waiting_time = n stop_arrival time - n-1 stop departure_time
        previous_stop_departure_time = None
        found_first_departure_time = False
        for stop_time_update in trip_update.stop_time_update:
            #we try to find the first valid stop_time_update with a valid departure_time
            #so we can calculate the waiting times of the successive ones
            row_write = ""
            if hasattr(stop_time_update,'departure'):
                if not found_first_departure_time:
                    found_first_departure_time = True
                else:
                    arrival_time = stop_time_update.arrival.time
                    waiting_time = arrival_time - previous_stop_departure_time
                    stop_id =  str(stop_time_update.stop_id)
                    batch.add(insert_stop, (trip_id,route_id,stop_id,waiting_time,arrival_time * 1000))
                    batch_counter = batch_counter + 1
                    #can only put 30 statements in a batch
                    if batch_counter == 30:                      
                        cassandra_session.execute(batch)
                        #create a new batch
                        batch = BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
                        batch_counter = 0
                previous_stop_departure_time = stop_time_update.departure.time    
    #execute remaining batch operations
    if batch_counter > 0: 
        cassandra_session.execute(batch)
    
    return {
        'statusCode': 200
    }

