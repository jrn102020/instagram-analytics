from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model


class RawUserEntity(Model):
    id = columns.Text(primary_key=True)
    username = columns.Text()
    full_name = columns.Text()
    profile_picture = columns.Text()
    bio = columns.Text()
    website = columns.Text()
    media_count = columns.Integer()
    follows = columns.Integer()
    followers = columns.Integer()

    def __repr__(self):
        return 'Id: %d - Username: %s - Media Count: %d - Follows: %d - Followers: %d' \
               % (self.id, self.username, self.media_count, self.follows, self.followers)

    @staticmethod
    def parse(single_user):

        try:
            id = single_user.id
        except:
            id = "NA"

        try:
            username = single_user.username
        except:
            username = "NA"

        try:
            full_name = single_user.full_name
        except:
            full_name = "NA"

        try:
            profile_picture = single_user.profile_picture
        except:
            profile_picture = "NA"

        try:
            bio = single_user.bio
        except:
            bio = "NA"

        try:
            website = single_user.website
        except:
            website = "NA"

        try:
            media_count = single_user.counts['media']
        except:
            media_count = -1

        try:
            follows = single_user.counts['follows']
        except:
            follows = 0

        try:
            followers = single_user.counts['followed_by']
        except:
            followers = 0

        return RawUserEntity(id=id, username=username, full_name=full_name,
                          profile_picture=profile_picture, bio=bio, website=website,
                          media_count=media_count, follows=follows, followers=followers)

    @staticmethod
    def sync_table():
        sync_table(RawUserEntity)
