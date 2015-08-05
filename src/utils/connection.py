from cassandra.cqlengine import connection
from instagram import client
from src.models.raw.raw_recent_media import RawRecentMediaEntity
from src.models.raw.raw_user import RawUserEntity
from src.models.raw.user_recent_media import RawUserRecentMediaEntity

INSTAGRAM_CONFIG = {
    'client_id': 'f5ac08e3237643f28361ffe36c1f6675',
    'client_secret': '64ff460f3f024b0e914be02eeddfddca',
    'redirect_uri': 'http://andrewzurn.com'
}
CASSANDRA_CONFIG = {
    'ip': 'localhost',
    'raw_keyspace': 'instagram_prototype_raw',
    'source_keyspace': 'instagram_prototype_source',
    'user': 'cassandra',
    'password': 'cassandra'
}

def open_source_session():
    connection.setup([CASSANDRA_CONFIG['ip']], CASSANDRA_CONFIG['source_keyspace'], protocol_version=3)
    #SourceRecentMediaEntity.sync_table()
    #SourceUserEntity.sync_table()
    #SourceUserRecentMediaEntity.sync_table()

def open_raw_session():
    connection.setup([CASSANDRA_CONFIG['ip']], CASSANDRA_CONFIG['raw_keyspace'], protocol_version=3)
    RawRecentMediaEntity.sync_table()
    RawUserEntity.sync_table()
    RawUserRecentMediaEntity.sync_table()

def generate_token_url(unauthenticated_api):
    url = unauthenticated_api.get_authorize_url(scope=["basic"])
    print "Please click the link below, login, and then provide" \
          " the 'code' value in the URL to 'code' variable into the next input"
    print "URL: " + url + "\n"

def get_instagram_api():
    unauthenticated_api = client.InstagramAPI(**INSTAGRAM_CONFIG)

    # Generate the oauth url for the user to log in with
    generate_token_url(unauthenticated_api)

    # Get our access token and open an api object
    code = raw_input("Please enter the code from the url from the browser you opened: ")
    try:
        auth_token, user_info = unauthenticated_api.exchange_code_for_access_token(code)
        if not auth_token:
            print 'Could not get access token'
        return client.InstagramAPI(access_token=auth_token, client_secret=INSTAGRAM_CONFIG['client_secret'])
    except Exception as e:
        print("Error: " + str(e))
        exit()
