import os
import string
from cassandra.cluster import Cluster, ConsistencyLevel, BatchStatement
from cassandra.query import BatchType

# To calculate waiting_time regarding a stop for a certain route for the current lambda function iteration
# we need to take into consideration the fact a bus route can have multiple busses at the same time
# Waiting_time of a stop for a route = arrival_time of the nearest bus of that route before that stop - current_time
# To achieve this we use a dictionary that stores the waiting time of the current nearest bus found
# Current nearest bus found is the one with the smallest waiting time
# Key = stop_id+route_id, since we want all the buses of the same route for that stop
# Value = [stop_id,route_id,waiting_time], to easily access every value for the insert
def get_stops(feed,current_time):
    stops = {}
    for entity in feed:
        trip_update = entity.trip_update
        trip = trip_update.trip
        route_id = int(trip.route_id)

        found_first_departure_time = False

        for stop_time_update in trip_update.stop_time_update:
            # we try to find the first valid stop_time_update with a valid departure_time
            # we also skip the first valid stop because its waiting_time is too optimistic
            # so we can calculate the waiting times of the successive ones

            if hasattr(stop_time_update, 'departure'):
                if not found_first_departure_time:
                    found_first_departure_time = True
                else:
                    stop_id = str(stop_time_update.stop_id)
                    arrival_time = stop_time_update.arrival.time
                    waiting_time = arrival_time - current_time

                    key = stop_id + str(route_id)
                    if key in stops:
                        if waiting_time < stops[key][2]:
                            stops[key][2] = waiting_time
                    else:
                        stops[key] = [stop_id, route_id, waiting_time]

    return stops


def insert_stops(stops, current_time, cassandra_session):
    cassandra_table = os.environ['CASSANDRA_TABLE']
    cassandra_row = os.environ['CASSANDRA_ROW']
    cassandra_prep_values = os.environ['CASSANDRA_PREP_VALUES']

    statement = "INSERT INTO " + cassandra_table + \
        " " + cassandra_row + " VALUES " + cassandra_prep_values
    insert_stop = cassandra_session.prepare(statement)
    batch = BatchStatement(batch_type=BatchType.UNLOGGED,
                           consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    # can only put 30 statements in a batch
    batch_counter = 0

    current_time = current_time * 1000
    for key in stops:
        stop = stops[key]
        stop_id = stop[0]
        route_id = stop[1]
        waiting_time = stop[2]

        batch.add(insert_stop, (stop_id, route_id, waiting_time, current_time))
        batch_counter = batch_counter + 1

        # can only put 30 statements in a batch
        if batch_counter == 30:
            cassandra_session.execute(batch)
            # create a new batch
            batch = BatchStatement(
                batch_type=BatchType.UNLOGGED, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            batch_counter = 0

    # execute remaining batch operations
    if batch_counter > 0:
        cassandra_session.execute(batch)
