import os

def execute_queries(athena):
    average_waiting_times_by_line(athena)
    average_waiting_times_by_location(athena)
    longest_waiting_time(athena)

def average_waiting_times_by_line(athena):
    query = 'WITH l AS( SELECT route_id, avg(waiting_time) as waiting_time \
    FROM "stops_feed"."stops_feed" \
    GROUP BY route_id ), \
    r AS( SELECT * FROM "stops_feed"."routes" ) \
    SELECT r.route_name, l.waiting_time \
    FROM l LEFT OUTER JOIN r \
    ON l.route_id = r.route_id'
    output = 'average_waiting_minutes/'
    execute_query(athena,query,output)


def average_waiting_times_by_location(athena):
    query = 'WITH w AS ( SELECT stop_id, route_id, sum(waiting_time) as waiting_time, count(*) as counter \
    FROM "stops_feed"."stops_feed" \
    GROUP BY stop_id, route_id ), \
    n AS ( SELECT * FROM "stops_feed"."locations" ) \
    SELECT n.location, sum(w.waiting_time)/sum(w.counter) \
    FROM w LEFT OUTER JOIN n \
    ON w.stop_id = n.stop_id \
    GROUP BY n.location'
    output = 'average_waiting_by_location'
    execute_query(athena,query,output)

def longest_waiting_time(athena):
    query = 'SELECT max(waiting_time) as waiting_time \
    FROM "stops_feed"."stops_feed"'
    output = 'longest_waiting_time'
    execute_query(athena,query,output)

def execute_query(athena,query,output):
    queryStart = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': os.environ['ATHENA_DB']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + os.environ['BUCKET_NAME'] + '/results/' + output,
        }
    )
