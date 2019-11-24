import sqlalchemy as db
from code.ihme import rebuild_ihme

if __name__ == '__main__':
    engine = db.create_engine('sqlite:///../data/RawHealthData.db')
    connection = engine.connect()

    rebuild_ihme(engine, connection)