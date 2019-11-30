from sqlalchemy import text

def initialize_db(engine):
    print("Initializing database..")
    sql = """
    SELECT table_name
      FROM information_schema.tables
     WHERE table_schema='public';
    """
    result_proxy = engine.execute(sql, con=engine)
    for row in result_proxy.fetchall():
        table = row[0]
        sql = text('DROP TABLE {}'.format(table))
        engine.execute(sql)