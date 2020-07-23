import os
import string
import csv
import boto3
import os
import time

# To calculate waiting_time regarding a stop for a certain route for the current lambda function iteration
# we need to take into consideration the fact a bus route can have multiple busses at the same time
# Waiting_time of a stop for a route = arrival_time of the nearest bus of that route before that stop - current_time
# To achieve this we use a dictionary that stores the waiting time of the current nearest bus found
# Current nearest bus found is the one with the smallest waiting time
# Key = stop_id+route_id, since we want all the buses of the same route for that stop
# Value = [stop_id,route_id,waiting_time], to easily access every value for the insert


def get_stops(feed, current_time):
    stops = {}
    for entity in feed:
        trip_update = entity.trip_update
        trip = trip_update.trip
        route_id = int(trip.route_id)

        found_first_arrival_time = False

        for stop_time_update in trip_update.stop_time_update:
            # we try to find the first valid stop_time_update with a valid arrival_time
            # we also skip the first valid stop because its waiting_time is too optimistic
            # so we can calculate the waiting times of the successive ones

            if hasattr(stop_time_update, 'arrival'):
                if not found_first_arrival_time:
                    found_first_arrival_time = True
                else:
                    arrival_time = stop_time_update.arrival.time
                    waiting_time = arrival_time - current_time
                    if waiting_time > 0 and waiting_time < 18000:
                        stop_id = str(stop_time_update.stop_id)
                        key = stop_id + str(route_id)
                        if key in stops:
                            if waiting_time < stops[key][2]:
                                stops[key][2] = waiting_time
                        else:
                            stops[key] = [stop_id, route_id, waiting_time]

    return stops


def insert_stops(stop_feed, current_time, s3, athena):
    with open('/tmp/stops_feed.csv', 'w+', newline='\n') as output:
        writer = csv.writer(output)
        for key in stop_feed:
            writer.writerow(stop_feed[key])

    #local time
    current_time = current_time + 7200
    date = time.strftime('%Y-%m-%d', time.localtime(current_time))
    hour = time.strftime('%H', time.localtime(current_time))
    bucket = os.environ['BUCKET_NAME']
    key = "date=" +  date + "/hour=" + hour + "/" + str(current_time) + ".csv"
    s3.Object(bucket , key).put(Body=open('/tmp/stops_feed.csv', 'rb'))

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
