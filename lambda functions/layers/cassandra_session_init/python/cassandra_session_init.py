from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
import os

def cassandra_session_init():
	cluster = os.environ['CASSANDRA_CLUSTER']
	username = os.environ['CASSANDRA_USERNAME']
	password = os.environ['CASSANDRA_PASSWORD']
	port = os.environ['CASSANDRA_PORT']
	ssl_context = SSLContext(PROTOCOL_TLSv1_2 )
	auth_provider = PlainTextAuthProvider(username=username, password=password)
	cluster = Cluster([cluster], load_balancing_policy=RoundRobinPolicy(), ssl_context=ssl_context, auth_provider=auth_provider, port=port)
	return cluster.connect()
