from urllib.request import urlopen
import json
import matplotlib.pyplot as plt
from peewee import *
from urllib.error import HTTPError
import threading
import time
import logging

logger = logging.getLogger(__name__)
hdlr = logging.StreamHandler()
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

DATABASE_PATH = "sonus.db"


db = SqliteDatabase(DATABASE_PATH)


class Play(Model):
    trackId = IntegerField(null=True)
    albumId = IntegerField(null=True)
    artistId = IntegerField(null=True)
    explicitLyrics = BooleanField(null=True)
    title = CharField()
    album = CharField()
    artist = CharField()
    trackDuration = IntegerField()
    startTime = TimestampField()
    endTime = TimestampField()
    station = CharField()

    class Meta:
        database = db  # This model uses the "radio.db" database.
        indexes = ((('station', 'startTime', 'endTime'), True), )


class IHeartFetcher():
    def __init__(self, station_num):
        self.db = SqliteDatabase(DATABASE_PATH)
        self.base = "https://us.api.iheart.com/api/v3/live-meta/stream/{station}/trackHistory"
        self.station_num = station_num
        self.url = self.base.format(station=station_num)


    def fetch(self, limit=20, offset=0):
        u = self.url + "?limit={limit}&offset={offset}".format(limit=limit,
                                                               offset=offset)
        try:
            content = urlopen(u).read().decode('utf-8')
        except UnicodeDecodeError:
            return None
        except HTTPError:
            return None


db = SqliteDatabase(DATABASE_PATH)


class Play(Model):
    trackId = IntegerField(null=True)
    albumId = IntegerField(null=True)
    artistId = IntegerField(null=True)
    explicitLyrics = BooleanField(null=True)
    title = CharField()
    album = CharField()
    artist = CharField()
    trackDuration = IntegerField()
    startTime = TimestampField()
    endTime = TimestampField()
    station = CharField()

    class Meta:
        database = db  # This model uses the "radio.db" database.
        indexes = ((('station', 'startTime', 'endTime'), True), )


class IHeartFetcher():
    def __init__(self, station_num):
        self.db = SqliteDatabase(DATABASE_PATH)
        self.base = "https://us.api.iheart.com/api/v3/live-meta/stream/{station}/trackHistory"
        self.station_num = station_num
        self.url = self.base.format(station=station_num)


    def fetch(self, limit=20, offset=0):
        u = self.url + "?limit={limit}&offset={offset}".format(limit=limit,
                                                               offset=offset)
        try:
            content = urlopen(u).read().decode('utf-8')
        except UnicodeDecodeError:
            return None
        except HTTPError:
            return None
        return json.loads(content)

    def store(self, limit=20, offset=0):
        # Insert rows 100 at a time.
        logger.debug("Starting fetch for station {station}".format(station=self.station_num))
        fields = ["albumId", "trackId", "artistId", "explicitLyrics", "title", "album", "artist", "trackDuration", "startTime", "endTime"]
        response = self.fetch(limit=limit, offset=offset)
        if (not response or "data" not in response):
            logger.info("No response for station {station}".format(
                station=self.station_num))
            return None
        plays = response["data"]
        data_source = [{
            **{field: play[field]
               for field in fields},
            **{
                "station": self.station_num
            }
        } for play in plays]

        BATCH_SIZE = 100
        inserted = 0
        logger.debug("Finished fetch for station {station}".format(station=self.station_num))
        with self.db.atomic():
            for idx in range(0, len(data_source), BATCH_SIZE):
                inserted += Play.insert_many(
                    data_source[idx:idx + BATCH_SIZE]).on_conflict(
                        action='IGNORE').execute()
        logger.info("Stored {inserted} plays for station {station}".format(
            station=self.station_num, inserted=inserted))
        return inserted


def get_intervals(data):
    return [(x["startTime"], x["endTime"]) for x in record["data"]]

def get_interduration(intervals):
    intervals = sorted(intervals, key=lambda x: x[0])
    return [intervals[i+1][0] - intervals[i][1] for i in range(len(intervals)-1) ]


if __name__ == "__main__":
    db.create_tables([Play])
    threads = []

    # KIIS: station 185
    for i in range(500):
        ihf = IHeartFetcher(station_num=i)
        thread = threading.Thread(target=ihf.store, kwargs={"limit": 5000, "offset":0} )
        threads.append(thread)

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
