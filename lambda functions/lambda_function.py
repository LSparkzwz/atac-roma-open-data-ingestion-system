import json
import trip_updates_feed as tuf
import cassandra_session_init as csi
import os
import string

def lambda_handler(event, context):
    cassandra_session = csi.cassandra_session_init()
    cassandra_table = os.environ['CASSANDRA_TABLE']
    cassandra_row = os.environ['CASSANDRA_ROW']
    
    batch_write_statement = "BEGIN BATCH "
    feed = tuf.update_feed()
    for entity in feed:
        row_write = " INSERT INTO " + cassandra_table + " " + cassandra_row
        trip_update = entity.trip_update
        trip = trip_update.trip
        vehicle = trip_update.vehicle
        
        trip_id = "'" + str(trip.trip_id) + "'"
        start_time = str(trip.start_time)
        start_date = str(trip.start_date)
        route_id = str(trip.route_id)
        vehicle_id = "'" + str(vehicle.id) + "'"
        vehicle_label = string.strip(vehicle.label)
        
        stu_string = "{"
        for stop_time_update in trip_update.stop_time_update:
            stu_string = stu_string + str(stop_time_update.stop_sequence) + " : '" + str(stop_time_update) + "'," 
        stu_string = stu_string[:-1] + "}"    
        
        row_write = row_write + " VALUES (" + trip_id + "," + start_time + "," + start_date + "," + route_id + "," + vehicle_id + "," + vehicle_label + "," + stu_string +")"
        batch_write_statement = batch_write_statement + row_write
        
    batch_write_statement = batch_write_statement + " APPLY BATCH"
        
    
    
    #r = cassandra_session.execute('select * from ' + cassandra_table)
    #print(batch_write_statement)
    
    return {
        'statusCode': 200
    }

