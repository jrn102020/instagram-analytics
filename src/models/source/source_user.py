from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model

class SourceUserEntity(Model):
    user_id = columns.Integer(primary_key=True)
    username = columns.Text(index=True)
    full_name = columns.Text(index=True)
    bio = columns.Text(index=True)
    locations = columns.List(columns.Text(index=True))
    website = columns.Text()
    media_count = columns.Integer(index=True)
    follows = columns.Integer(index=True)
    followers = columns.Integer(index=True)
    recent_media_ids = columns.List(columns.Text)
    most_recent_engagement_rating = columns.Float(index=True)
    averaged_engagement_rating = columns.Float(index=True)
    trending = columns.Boolean(index=True)
    trending_value = columns.Float(index=True)
    created_time = columns.DateTime(index=True)
    updated_time = columns.DateTime(index=True)

    def __repr__(self):
        return 'Id: %d - Username: %s - Followers: %d - RecentEngagementRating: %0.3f' \
               ' - AveragedRating: %0.3f - Trending: %r - Value: %0.2f' \
               % (self.user_id, self.username, self.followers, self.most_recent_engagement_rating,
                  self.averaged_engagement_rating, self.trending, self.trending_value)

    def tsv_repr(self):
        return(str(self.user_id).encode('utf-8') + "\t" +
              self.username.replace("\n","").replace("\t","").encode('utf-8') + "\t" +
              self.full_name.replace("\n","").replace("\t","").encode('utf-8') + "\t" +
              self.bio.replace("\n","").replace("\t","").encode('utf-8') + "\t" +
              ",".join(self.locations).replace("\n","").replace("\t","").encode('utf-8') + "\t" +
              self.website.replace("\n","").replace("\t","").encode('utf-8') + "\t" +
              str(self.media_count).encode('utf-8') + "\t" +
              str(self.follows).encode('utf-8') + "\t" +
              str(self.followers).encode('utf-8') + "\t" +
              ",".join(self.recent_media_ids).encode('utf-8') + "\t" +
              str(self.most_recent_engagement_rating).encode('utf-8') + "\t" +
              str(self.averaged_engagement_rating).encode('utf-8') + "\t" +
              str(self.trending).encode('utf-8') + "\t" +
              str(self.trending_value).encode('utf-8') + "\t" +
              str(self.created_time).encode('utf-8') + "\t" +
              str(self.updated_time).encode('utf-8') + "\n")

    @staticmethod
    def tsv_header():
        return("USER_ID\t"
              "USER_NAME\t"
              "FULL_NAME\t"
              "BIO\t"
              "LOCATIONS\t"
              "WEBSITE\t"
              "MEDIA_COUNT\t"
              "FOLLOWS\t"
              "FOLLOWERS\t"
              "RECENT_MEDIA_IDS\t"
              "RECENT_ENGAGEMENT_RATING\t"
              "AVG_ENGAGEMENT_RATING\t"
              "TRENDING\t"
              "TRENDING_VALUE\t"
              "CREATED_TIME\t"
              "UPDATED_TIME\n")

    @staticmethod
    def sync_table():
        sync_table(SourceUserEntity)
