from sqlalchemy import text
import pandas as pd

def clean_wdi_population(engine):
    wdi_population = pd.read_sql_table('wdi_population', con=engine)
    wdi_population_pivotized = pd.melt(wdi_population, id_vars=['Country Code', 'Indicator Code'],
                 value_vars=[str(val) for val in range(1960, 2019, 1)]).dropna()
    wdi_population_pivotized = wdi_population_pivotized.dropna().reset_index(drop=True)
    sql = "DROP TABLE IF EXISTS wdi_population_pivotized CASCADE ;"
    sql = text(sql)
    _ = engine.execute(sql)
    wdi_population_pivotized.to_sql('wdi_population_pivotized', con=engine)
    sql = """\
        DROP VIEW IF EXISTS wdi_population_pivotized_cleaned;

        CREATE VIEW wdi_population_pivotized_cleaned AS
        SELECT "Country Code" AS "Code",
               variable AS "Year",
               value as "Population"
        FROM wdi_population_pivotized;
        """
    sql = text(sql)
    _ = engine.execute(sql)