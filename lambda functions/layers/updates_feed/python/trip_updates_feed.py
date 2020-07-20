from google.transit import gtfs_realtime_pb2
from urllib.request import urlopen

def update_feed():
	feed = gtfs_realtime_pb2.FeedMessage()
	response = urlopen('https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb')
	feed.ParseFromString(response.read())
	return feed.entity
