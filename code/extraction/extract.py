
from sqlalchemy import create_engine

from code.extraction.load_csvs import load_csv
from code.extraction.normalize_ihme import normalize_ihme
from code.extraction.normalize_wdi import normalize_wdi
from code.config import username, password, host, port, database, data_dir
engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, database))

INITIALIZE_DB = True

if __name__ == '__main__':
    if INITIALIZE_DB:
        print("Initializing database..")
        sql = """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema='public';
        """
        result_proxy = engine.execute(sql, con=engine)
        for row in result_proxy.fetchall():
            table = row[0]
            sql = "DROP TABLE {}".format(table)
            engine.execute(sql)

    load_csv(data_dir, engine)
    normalize_ihme(engine)
    normalize_wdi(engine)
    print("Finished extraction of data sources.")