from google.transit import gtfs_realtime_pb2
import urllib

def update_feed():
	feed = gtfs_realtime_pb2.FeedMessage()
	response = urllib.urlopen('https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb')
	feed.ParseFromString(response.read())
	return feed.entity
