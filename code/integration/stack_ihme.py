import pandas as pd

def stack_ihme(engine):
    connection = engine.connect()
    # get all ihme tables that need to be united
    sql = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    AND table_name LIKE 'ihme_gbd%%'
    ;
    """
    df = pd.read_sql_query(sql, engine)
    ihme_tables = df['table_name'].values

    # filter values for the ihme tables
    # only rows containing information about the following topics will be addressed
    # reduces ihme content from 360 diseases to the 27 relevant for smoking
    filter_values = [
        "=          'Esophageal cancer'",
        "=          'Liver cancer'",
        "=          'Ectopic pregnancy'",
        "=          'Tuberculosis'",
        "=          'Rheumatoid arthritis'",
        "=          'Bladder cancer'",
        "=          'Cervical cancer'",
        "=          'Psoriaris'",
        "LIKE       '%%diabetes%%'",
        "LIKE       '%%heart%%'",
        "LIKE       '%%vision%%'",
        "LIKE       '%%lung%%'",
        "LIKE       '%%stroke%%'",
        "LIKE       '%%hearing%%'",
        "LIKE       '%%dementia%%'"
    ]
    sql = 'SELECT key AS cause_id, value AS disease FROM cause \nWHERE' \
          + '\n   OR'.join([' value {}'.format(filter_value) for filter_value in filter_values]) \
          + ';'
    result_set = connection.execute(sql).fetchall()

    # ihme tables will be filtered for the following cause ids
    cause_ids = ', '.join([str(row[0]) for row in result_set])

    # unneccessary values will be deleted from the lookup table, indicating which ihme tables won't be needed nomore
    sql = 'DELETE FROM cause WHERE key NOT IN ({});'.format(cause_ids)
    connection.execute(sql);

    # build the sql string by concatenating SELECTs using UNION
    sql = '\nUNION\n'.join(
        ["SELECT * FROM {} WHERE cause_id IN ({})".format(table_name, cause_ids) for table_name in ihme_tables]
    )
    ihme_df = pd.read_sql_query(sql, engine)
    ihme_df.to_sql('ihme_smoking_diseases', con=engine, if_exists='replace');