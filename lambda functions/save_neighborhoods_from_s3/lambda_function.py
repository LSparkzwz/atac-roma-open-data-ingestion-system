from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
from google.transit import gtfs_realtime_pb2
import requests
import boto3
import os
import csv

s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
	username = os.environ['CASSANDRA_USER']
	password = os.environ['CASSANDRA_PWD']
	ssl_context = SSLContext(PROTOCOL_TLSv1_2 )
	auth_provider = PlainTextAuthProvider(username=username, password=password)
	cluster = Cluster(['cassandra.us-east-1.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142)
	session = cluster.connect()
	stmt = session.prepare("INSERT INTO neighborhood_stops.stops (stop_id, stop_code, stop_name, lat, lon, neighborhood) VALUES (?, ?, ?, ?, ?, ?)")
	execution_profile = session.execution_profile_clone_update(session.get_execution_profile(EXEC_PROFILE_DEFAULT))
	execution_profile.consistency_level = ConsistencyLevel.LOCAL_QUORUM
	
	bucket = 'bigdatafilippo'
	key = 'quartieri.csv'
	response = s3.get_object(Bucket=bucket, Key=key)
	content = response.get('Body').read().decode('utf-8')
	reader = csv.reader(content.splitlines(), delimiter=',')
	rows=list(reader)
	count = 0
	for row in rows:
		stop_id = row[1]
		stop_code = row[2]
		stop_name = row[3]
		lat = row[4]
		lon = row[5]
		neighborhood = row[6]
		notFound = 'Non trovato'
		if(neighborhood != notFound):
			session.execute(stmt, (stop_id,stop_code,stop_name,lat,lon,neighborhood), execution_profile=execution_profile)
		count+=1
	print('count ' + str(count))
	