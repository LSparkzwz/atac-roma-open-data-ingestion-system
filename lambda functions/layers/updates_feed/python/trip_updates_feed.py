from google.transit import gtfs_realtime_pb2
from urllib.request import urlopen
import os

def update_feed():
	feed = gtfs_realtime_pb2.FeedMessage()
	trip_updates_feed_url = os.environ['TRIP_UPDATES_FEED_URL']
	response = urlopen(trip_updates_feed_url)
	feed.ParseFromString(response.read())
	return feed.entity
