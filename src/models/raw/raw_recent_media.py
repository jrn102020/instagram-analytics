from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
import datetime


class RawRecentMediaEntity(Model):
    id = columns.Text(primary_key=True)
    user_id = columns.Integer(index=True)
    username = columns.Text()
    full_name = columns.Text()
    caption_text = columns.Text()
    likes_count = columns.Integer(index=True)
    comments = columns.List(columns.Text())
    comment_count = columns.Integer(index=True)
    tags = columns.List(columns.Text())
    url = columns.Text()
    date_time = columns.DateTime(index=True)
    location = columns.Text(index=True)

    def __repr__(self):
        return 'Id: %s - Username: %s - UserId: %d - LikesCount: %d - CommentsCount: %d - Timestamp: %s - Location: %s' \
               % (self.id, self.username, self.user_id, self.likes_count, self.comment_count, self.date_time, self.location)

    @staticmethod
    def parse(single_media):

        try:
            id = single_media.id
        except:
            id = "NA"

        # get the full name of the user
        try:
            full_name = single_media.user.full_name
        except:
            full_name = "NA"

        # Get the username
        try:
            username = single_media.user.username
        except:
            username = "NA"

        #Get the user id
        try:
            user_id = single_media.user.id
        except:
            user_id = -1

        # Get the Caption
        try:
            caption_text = single_media.caption.text
        except:
            caption_text = "NA"

        # Get the Like Count
        try:
            likes_count = single_media.like_count
        except:
            likes_count = 0

        # Get the comments
        try:
            comments = []
            for comment in single_media.comments:
                comments.append(comment.text)
        except:
            comments = []

        # Get the Comment Count
        try:
            comment_count = single_media.comment_count
        except:
            comment_count = 0

        # Get the Url
        try:
            url = single_media.link
        except:
            url = "NA"

        # Get the tags
        try:
            tags = []
            for tag in single_media.tags:
                tags.append(tag.name)
        except:
            tags = []

        # Get the Timestamp
        try:
            date_time = single_media.created_time
        except:
            date_time = datetime.datetime.now()

        # Get the location
        try:
            location = single_media.location.name
        except:
            location = "NA"

        return RawRecentMediaEntity(id=id, username=username, full_name=full_name, user_id=user_id,
                           caption_text=caption_text, likes_count=likes_count, comments=comments,
                           comment_count=comment_count, tags=tags, url=url, date_time=date_time, location=location)

    @staticmethod
    def sync_table():
        sync_table(RawRecentMediaEntity)
