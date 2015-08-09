# System / Date Time
import datetime

# Schedule
import langdetect
from langdetect.lang_detect_exception import LangDetectException
import schedule
import time

# Model classes
from src.models.raw.raw_recent_media import RawRecentMediaEntity
from src.models.raw.raw_user import RawUserEntity
from src.models.raw.raw_user_recent_media import RawUserRecentMediaEntity
from src.models.source.source_user import SourceUserEntity
from src.utils.connection import open_cassandra_session, get_instagram_api


def produce_raw_layer():
    try:
        print 'START: Insert of data into database at %s.' % datetime.datetime.now()
        cycle_start_time = datetime.datetime.now()
        recent_media_added = 0
        users_added = 0
        user_recent_media_added = 0

        # Get recent popular media
        recent_media = api.media_popular(count=64)
        for media in recent_media:
            # Parse the recent popular media
            parsed_media = RawRecentMediaEntity.parse(media)

            # Determine if english speaking user, if so, continue
            ## TODO: Maybe detect all possible languages and then if 'en' is in it, it passes
            try:
                if langdetect.detect(parsed_media.caption_text) != 'en':
                    continue
            except LangDetectException:
                continue

            # Save the parsed media
            parsed_media.save()
            recent_media_added += 1

            user_recent_media_added, users_added = handle_user_info(parsed_media, user_recent_media_added, users_added)

        log_run_metrics(cycle_start_time, recent_media_added, users_added, user_recent_media_added)
    except Exception as e:
        print("ERROR - userId: %d caused error: " + str(e))
        pass


def handle_user_info(parsed_media, user_recent_media_added, users_added):
    # Find the user info
    user = RawUserEntity.parse(api.user(parsed_media.user_id))
    user.save()
    users_added += 1

    # Find and parse the users recent media
    user_recent_media = []
    recents = api.user_recent_media(user_id=parsed_media.user_id, count=64)
    for recent in recents[0]:
        parsed_user_recent_media = RawUserRecentMediaEntity.parse(recent)
        parsed_user_recent_media.save()
        user_recent_media.append(parsed_user_recent_media)
        user_recent_media_added += 1

    source_user_model_obj = SourceUserEntity.objects(SourceUserEntity.user_id == user.user_id)
    if source_user_model_obj.first():
        update_source_user(source_user_model_obj)
    else:
        save_source_user(user, user_recent_media)

    return user_recent_media_added, users_added


def update_source_user(source_user_obj):
    source_user = source_user_obj.first()
    source_user.update_time = datetime.datetime.now()
    source_user.update()
    print("UPDATE user: " + repr(source_user))


def save_source_user(user, user_recent_media):
    # Compute the additional values for the user, and a save as source user
    most_recent_engagement_rating = engagement_rating(user_recent_media[0: 1], user.followers)
    averaged_engagement_rating = engagement_rating(user_recent_media, user.followers)
    is_trending, trending_value = trending(user_recent_media, user.followers)
    locations = find_location(user_recent_media)
    recent_media_ids = map(lambda media: media.id, user_recent_media)

    source_user = SourceUserEntity(user_id=user.user_id, username=user.username, full_name=user.full_name,
                                   bio=user.bio, locations=locations, website=user.website,
                                   media_count=user.media_count,
                                   follows=user.follows, followers=user.followers, recent_media_ids=recent_media_ids,
                                   most_recent_engagement_rating=most_recent_engagement_rating,
                                   averaged_engagement_rating=averaged_engagement_rating, trending=is_trending,
                                   trending_value=trending_value, created_time=datetime.datetime.now(),
                                   updated_time=datetime.datetime.now())
    print("INSERT user: " + repr(source_user))
    source_user.save()


# Sums the likes and the counts of the most recent list divided by the count of the
# most recent media list to produce the average like and count sum. Then takes that
# value and divides it by the total amount of followers to produce an averaged
# engagement rating for the user's previous N media posts.
#
# Returns a float of the averaged engagement rating value.
def engagement_rating(user_recent_media_list, total_followers):
    sum = 0
    for media in user_recent_media_list:
        sum += media.likes_count + media.comment_count

    averaged_engagements = sum / float(len(user_recent_media_list))

    return averaged_engagements / float(total_followers)


# Will compare the users previous recent media by producing an engagement rating
# of the user's most recent (half the list) and their previous (2nd half of list)
# recent media, and then divide those two values to produce growth indicator or
# a trending value between there previous media to their most recent media.
#
# Returns a boolean (true if trending_rating > 0.5), and their trending value (float)
def trending(recent_media_list, total_followers):

    # Try to take the last 10 media items
    if len(recent_media_list) > 10:
        most_recents = recent_media_list[0: 5]
        previous_recents = recent_media_list[5: 10]
    else: # split the elements in half
        most_recents = recent_media_list[0: (len(recent_media_list) / 2)]
        previous_recents = recent_media_list[(len(recent_media_list) / 2): len(recent_media_list)]

    most_recents_engagement_rating = engagement_rating(most_recents, total_followers)
    previous_recent_engagement_rating = engagement_rating(previous_recents, total_followers)

    trending_rating = ((most_recents_engagement_rating - previous_recent_engagement_rating) / most_recents_engagement_rating)
    is_trending = trending_rating > 0.0

    return is_trending, trending_rating


def find_location(recents):
    locations = []
    for recent in recents:
        recent_location = recent.location
        if recent_location != "NA" and recent_location != "":
            locations.append(recent_location)

    return set(locations)


def log_run_metrics(cycle_start_time, recent_media_added, users_added, user_recent_media_added):
    print '\nDONE: inserting new records into database at %s.' % datetime.datetime.now()
    print 'Inserted: RecentMedia: %d -- Users: %d -- UserRecentMedia: %d' % (recent_media_added,\
users_added, user_recent_media_added)
    print 'Cycle run time taken: %s' % (datetime.datetime.now() - cycle_start_time)
    print 'Process start time was: %s' % process_start_time
    print 'Process run time currently: %s' % (datetime.datetime.now() - process_start_time)


if __name__ == '__main__':
    api = get_instagram_api()

    # Open cluster connection to the raw keyspace, and build/connect to our tables
    open_cassandra_session()

    # Schedule our job, and begin it, sleeping for 120 seconds between each job before rerunning
    print 'Scheduling job for every 140 seconds, time is %s.' % datetime.datetime.now()
    schedule.every(140).seconds.do(produce_raw_layer)

    # Process start time
    process_start_time = datetime.datetime.now()

    while True:
        schedule.run_all()
        time.sleep(120)
