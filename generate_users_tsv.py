import datetime
from src.models.source.source_user import SourceUserEntity
from src.utils.connection import open_cassandra_session

if __name__ == '__main__':
    open_cassandra_session()

    tsv_file = open('SOURCE_USERS_%s.txt' % str(datetime.datetime.now().strftime("%Y-%m-%d")), 'w')
    tsv_file.write(SourceUserEntity.tsv_header())
    users = SourceUserEntity.all()
    for user in users:
        print(user.tsv_repr())
        tsv_file.write(user.tsv_repr())

    tsv_file.flush()
    tsv_file.close()
