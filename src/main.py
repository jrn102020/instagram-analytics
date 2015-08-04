# Instagram
from instagram import client

# Cassandra
from cassandra.cqlengine import connection

import os
import datetime

# Schedule
import langid
import schedule
import time

# Custom classes
from src.media import MediaEntity
from src.user import UserEntity
from src.user_recent_media import UserRecentMediaEntity


def generate_token_url(unauthenticated_api):
    url = unauthenticated_api.get_authorize_url(scope=["basic"])
    print "Please click the link below, login, and then provide" \
          " the 'code' value in the URL to 'code' variable into the next input"
    print "URL: " + url + "\n"


def open_cassandra_session():
    connection.setup([CASSANDRA_CONFIG['ip']], CASSANDRA_CONFIG['schema'], protocol_version=3)
    MediaEntity.sync_table()
    UserEntity.sync_table()
    UserRecentMediaEntity.sync_table()


def handle_raw_popular_media():
    print 'Beginning insert of data into database at %s.' % datetime.datetime.now()

    # Get recent popular media
    print 'Calling popular media at %s.' % datetime.datetime.now()
    recent_media = api.media_popular(count=64)
    for media in recent_media:

        # Parse the recent popular media
        parsed_media = MediaEntity.parse(media)

        # Determine if english speaking user, if so, continue
        if langid.classify(parsed_media.caption_text)[0] != 'en':
            continue

        # Find the user info
        print 'Calling get user at %s.' % datetime.datetime.now()
        parsed_user = UserEntity.parse(api.user(parsed_media.user_id))
        parsed_user.save()

        # Find and parse the users recent media
        user_recent_medias = api.user_recent_media(user_id=parsed_media.user_id, count=64)
        for user_recent_media in user_recent_medias[0]:
            print 'Calling get user recent media at %s.' % datetime.datetime.now()
            parsed_user_recent_media = UserRecentMediaEntity.parse(user_recent_media)
            parsed_user_recent_media.save()

    print 'Inserted new records into database at %s.\n\n' % datetime.datetime.now()

if __name__ == '__main__':
    # will be assigned to our instagram api object when intialized
    api = ''

    # will hold the auth token that we exchange the instagram code for
    auth_token = ''

    # Set an os env to allow our program to manipulate the cassandra schemas
    os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = 'YES'

    INSTAGRAM_CONFIG = {
        'client_id': 'f5ac08e3237643f28361ffe36c1f6675',
        'client_secret': '64ff460f3f024b0e914be02eeddfddca',
        'redirect_uri': 'http://andrewzurn.com'
    }

    CASSANDRA_CONFIG = {
        'ip': 'localhost',
        'schema': 'instagram_prototype',
        'user': 'cassandra',
        'password': 'cassandra'
    }

    # Open cluster connection, and build/connect to our tables
    open_cassandra_session()

    # Generate the oauth url for the user to log in with
    unauthenticated_api = client.InstagramAPI(**INSTAGRAM_CONFIG)
    generate_token_url(unauthenticated_api)

    # Get our access token and open an api object
    code = raw_input("Please enter the code from the url from the browser you opened: ")
    try:
        auth_token, user_info = unauthenticated_api.exchange_code_for_access_token(code)
        if not auth_token:
            print 'Could not get access token'
        api = client.InstagramAPI(access_token=auth_token, client_secret=INSTAGRAM_CONFIG['client_secret'])
    except Exception as e:
        print("Error: " + str(e))
        exit()

    # This is where we could schedule our job
    print 'Scheduling job for every 140 seconds, time is %s.' % datetime.datetime.now()
    schedule.every(140).seconds.do(handle_raw_popular_media)

    while True:
        schedule.run_all()
        time.sleep(120)
