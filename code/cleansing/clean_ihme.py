import pandas as pd
from sqlalchemy import text

def clean_ihme(engine):
    print("Cleaning IHME data")
    # fill x_mapping and x_delete values
    sql = """\
    TRUNCATE TABLE x_mapping;

    -- age lookups
    INSERT INTO x_mapping VALUES ('<1 year', 'Birth', 'age');
    INSERT INTO x_mapping VALUES ('Under 5', '1 to 4', 'age');
    INSERT INTO x_mapping VALUES ('70+ years', '70+', 'age');
    INSERT INTO x_mapping VALUES ('1 to 4', '01 to 04', 'age');
    INSERT INTO x_mapping VALUES ('5 to 9', '05 to 09', 'age');
    INSERT INTO x_mapping VALUES ('5-14 years', '05 to 14', 'age');
    INSERT INTO x_mapping VALUES ('15-49 years', '15 to 49', 'age');
    INSERT INTO x_mapping VALUES ('50-69 years', '50 to 69', 'age');
    INSERT INTO x_mapping VALUES ('80 plus', '70+', 'age');
    INSERT INTO x_mapping VALUES ('95 plus', '95+', 'age');

    INSERT INTO x_delete VALUES('Age-standardized', 'age');
    INSERT INTO x_delete VALUES('<70 years', 'age');
    INSERT INTO x_delete VALUES('<20 years', 'age');

    -- measure lookups
    INSERT INTO x_mapping VALUES ('YLDs (Years Lived with Disability)', 'Years lived with disability', 'measure');
    INSERT INTO x_mapping VALUES ('DALYs (Disability-Adjusted Life Years)', 'Disability-adjusted life years', 'measure');
    INSERT INTO x_mapping VALUES ('YLLs (Years of Life Lost)', 'Years of life lost', 'measure');
    """
    sql = text(sql)
    _ = engine.execute(sql)

    # read them as a pandas file, update from old to new values
    # doing this extra step to get a table which contains our mappings (as a legacy value lookup)
    x_mapping = pd.read_sql_table('x_mapping', con=engine)
    template = "UPDATE <table_name> SET value = '<new_value>' WHERE value = '<old_value>';"
    for idx, row in x_mapping.iterrows():
        old_value, new_value, table_name = row
        sql = template.replace('<table_name>', table_name) \
            .replace('<old_value>', old_value) \
            .replace('<new_value>', new_value)
        sql = text(sql)
        engine.execute(sql)

    x_delete = pd.read_sql_table('x_delete', con=engine)
    template = "DELETE FROM <table_name> WHERE value = '<value>';"
    for idx, row in x_delete.iterrows():
        value, table_name = row
        sql = template.replace('<table_name>', table_name) \
            .replace('<value>', value)
        sql = text(sql)
        engine.execute(sql)

    # clean IHME
    delete_template = """\
    DELETE FROM ihme_smoking_diseases ihme
    WHERE <lkp_name>_id NOT IN (SELECT key FROM <lkp_name>);
    """

    # statistics about lookup tables:
    view_template = """\
    DROP VIEW IF EXISTS <lkp_name>_occurences;

    CREATE VIEW <lkp_name>_occurences AS
    SELECT value, COUNT(<lkp_name>.key) FROM ihme_smoking_diseases ihme
    INNER JOIN <lkp_name>
    ON ihme.<lkp_name>_id = <lkp_name>.key
    GROUP BY <lkp_name>.value;
    """

    lkp_names = ['age', 'cause', 'measure', 'metric', 'location', 'sex']
    for lkp_name in lkp_names:
        delete_sql = delete_template.replace('<lkp_name>', lkp_name)
        delete_sql = text(delete_sql)
        engine.execute(delete_sql)
        view_sql = view_template.replace('<lkp_name>', lkp_name)
        view_sql = text(view_sql)
        engine.execute(view_sql)

    # IHME as view
    sql = """\
    DROP VIEW IF EXISTS ihme_smoking_diseases_cleaned;

    CREATE VIEW ihme_smoking_diseases_cleaned AS
    SELECT * FROM ihme_smoking_diseases;
    """
    sql = text(sql)
    _ = engine.execute(sql)
    print("Finished cleaning IHME data")