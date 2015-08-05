# System / Date Time
import datetime

# Schedule
import langdetect
import schedule
import time

# Model classes
from src.models.raw.raw_recent_media import RawRecentMediaEntity
from src.models.raw.raw_user import RawUserEntity
from src.models.raw.user_recent_media import RawUserRecentMediaEntity
from src.utils.connection import open_raw_session, get_instagram_api
from src.utils.setup import setup_env

def produce_raw_layer():
    print 'Beginning insert of data into database at %s.' % datetime.datetime.now()

    # Get recent popular media
    print 'Calling popular media at %s.' % datetime.datetime.now()
    recent_media = api.media_popular(count=64)
    for media in recent_media:

        # Parse the recent popular media
        parsed_media = RawRecentMediaEntity.parse(media)

        # Determine if english speaking user, if so, continue
        if langdetect.detect(parsed_media.caption_text) != 'en': ## TODO: Maybe detect all possible languages and then if 'en' is in it, it passes
            continue

        # Save the parsed media
        parsed_media.save()

        # Find the user info
        print 'Calling get user at %s.' % datetime.datetime.now()
        parsed_user = RawUserEntity.parse(api.user(parsed_media.user_id))
        parsed_user.save()

        # Find and parse the users recent media
        user_recent_medias = api.user_recent_media(user_id=parsed_media.user_id, count=64)
        for user_recent_media in user_recent_medias[0]:
            print 'Calling get user recent media at %s.' % datetime.datetime.now()
            parsed_user_recent_media = RawUserRecentMediaEntity.parse(user_recent_media)
            parsed_user_recent_media.save()

    print 'Inserted new records into database at %s.\n\n' % datetime.datetime.now()

if __name__ == '__main__':
    setup_env()

    # Open cluster connection to the raw keyspace, and build/connect to our tables
    open_raw_session()

    api = get_instagram_api()

    # Schedule our job, and begin it, sleeping for 120 seconds between each job before rerunning
    print 'Scheduling job for every 140 seconds, time is %s.' % datetime.datetime.now()
    schedule.every(140).seconds.do(produce_raw_layer)

    while True:
        schedule.run_all()
        time.sleep(120)
