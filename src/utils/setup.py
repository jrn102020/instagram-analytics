import os

# Set an os env to allow our program to manipulate the cassandra schemas
def setup_env():
    os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = 'YES'