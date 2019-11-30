# TODO: script to clean tables which are not needed anymore
from sqlalchemy import text

def clean_db(engine, table_names):

    print("Cleaning database from unneccessary tables")
    for table in table_names:
        sql = text('DROP TABLE {}'.format(table))
        engine.execute(sql)