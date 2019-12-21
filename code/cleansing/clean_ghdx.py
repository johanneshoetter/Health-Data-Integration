from sqlalchemy import text
import pandas as pd

def clean_ghdx(engine):
    print("Cleaning GHDx data")
    # cleaning share of adults who smoke by level of prosperity
    sql = """\
    DROP VIEW IF EXISTS  share_of_adults_smoking_by_level_of_prosperity_cleaned;

    CREATE VIEW share_of_adults_smoking_by_level_of_prosperity_cleaned AS
    SELECT "Entity", "Code", "Year",
           "Adults who smoke (%)" AS "Adults who smoke (in %)",
           ROUND("GDP per capita (international-$) (constant 2011 international $") AS "GDP per capita"
    FROM share_of_adults_who_are_smoking_by_level_of_prosperity
    WHERE "Adults who smoke (%)" IS NOT NULL
    OR "GDP per capita (international-$) (constant 2011 international $" IS NOT NULL;
    """
    sql = text(sql)
    _ = engine.execute(sql)

    # cleaning smoking deaths by age:
    sql = """\
    DROP VIEW IF EXISTS smoking_deaths_by_age_cleaned;

    CREATE VIEW smoking_deaths_by_age_cleaned AS
    SELECT "Entity", "Code", "Year",
           ROUND("15-49 years old (deaths)") AS "15 to 49",
           ROUND("50-69 years old (deaths)") AS "50 to 69",
           ROUND("70+ years old (deaths)") AS "70+"
    FROM smoking_deaths_by_age;
    """
    sql = text(sql)
    _ = engine.execute(sql)

    # Cleaning ghdx_measures by renaming column names first and neglecting irrelevant columns
    mappings = {
        "Indicator:Average -  cigarette price in international dollars (": "Average cigarette price",
        "Share of women (% of women)": "Share of women (in %)",
        "Share of men (% of men)": "Share of men (in %)",
        "Cigarette consumption per smoker per day (cigarettes)": "Estimated daily cigarette consumption per smoker",
        "Estimated daily consumption (cigarettes)": "Estimated daily cigarette consumption",
        "Upper bound (cigarettes)": "Upper bound (cigarettes)",
        "Lower bound (cigarettes)": "Lower bound (cigarettes)",
        "Indicator:Enforce bans on tobacco advertising": "Bans on tobacco advertising",
        "Secondhand smoke": "Secondhand smoke",
        "Tobacco smoking": "Tobacco smoking",
        "Sales of cigarettes per adult per day (International Smoking St": "Daily cigarette sales per smoker",
        "Smoking prevalence, total (ages 15+) (% of adults)": "Smoking prevalence (in %)",
        "Age-standardized share of cancer deaths attributed to tobacco (": "Share of cancer deaths attribute to tobacco (in %)",
        "Indicator:Raise taxes on tobacco": "Taxes on tobacco",
        "Smoking (deaths)": "Deaths by smoking",
        "Secondhand smoke (deaths)": "Deaths by secondhand smoke",
        "Indicator:Offer help to quit tobacco use": "Offer to help quitting",
        "Indicator:Average - taxes as a % of cigarette price - total tax": "Average taxes as % of cigarette price"
    }
    sql = """\
    DROP VIEW IF EXISTS ghdx_measures_cleaned;

    CREATE VIEW ghdx_measures_cleaned AS 
        SELECT "Code", "Year",\n
    """
    for old_column_name, new_column_name in mappings.items():
        sql += '\tROUND("{}") AS "{}",\n'.format(old_column_name, new_column_name)
    sql = sql[:-2] + "\nFROM ghdx_measures\n"  # getting rid of the last ,\n
    for idx, old_column_name in enumerate(mappings.keys()):
        if idx == 0:
            sql += '\nWHERE "{}" IS NOT NULL'.format(old_column_name)
        else:
            sql += '\nOR "{}" IS NOT NULL'.format(old_column_name)
    sql += ';'
    sql = text(sql)
    _ = engine.execute(sql)

    # ghdx measures pivotized as view
    ghdx_cleaned = pd.read_sql_table('ghdx_measures_cleaned', con=engine)
    ghdx_cleaned_pivotized = pd.melt(ghdx_cleaned, id_vars=["Code", "Year"],
                                     value_vars=[new_column_name for new_column_name in mappings.values()])
    ghdx_cleaned_pivotized = ghdx_cleaned_pivotized.dropna().reset_index(drop=True)
    sql = "DROP TABLE IF EXISTS ghdx_measures_pivotized CASCADE ;"
    sql = text(sql)
    _ = engine.execute(sql)
    ghdx_cleaned_pivotized.to_sql('ghdx_measures_pivotized', con=engine)
    sql = """\
    DROP VIEW IF EXISTS ghdx_measures_pivotized_cleaned;

    CREATE VIEW ghdx_measures_pivotized_cleaned AS
    SELECT "Code", "Year",
           variable AS "Metric",
           value as "Measure"
    FROM ghdx_measures_pivotized;
    """
    sql = text(sql)
    _ = engine.execute(sql)
    print("Finished cleaning GHDx data")