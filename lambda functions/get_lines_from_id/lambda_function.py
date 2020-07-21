from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
from google.transit import gtfs_realtime_pb2
import requests
from zipfile import ZipFile
from io import BytesIO
import os

def lambda_handler(event, context):
	username = os.environ['CASSANDRA_USER']
	password = os.environ['CASSANDRA_PWD']
    response = requests.get('https://romamobilita.it/sites/default/files/rome_static_gtfs.zip').content
    zipfile = ZipFile(BytesIO(response))
    ssl_context = SSLContext(PROTOCOL_TLSv1_2 )
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    cluster = Cluster(['cassandra.us-east-1.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142)
    session = cluster.connect()
    stmt = session.prepare("INSERT INTO static_files.lines_correspondence (route_id, actual_line_name) VALUES (?, ?)")
    execution_profile = session.execution_profile_clone_update(session.get_execution_profile(EXEC_PROFILE_DEFAULT))
    execution_profile.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    with zipfile.open('routes.txt', 'r') as myfile:
        #print(myfile.read())
        content = myfile.readlines()
        for elem in content:
            elemList=elem.decode().split(sep=',')
            elemList[2] = elemList[2].replace('"','')
            session.execute(stmt, (elemList[0],elemList[2]), execution_profile=execution_profile)

    