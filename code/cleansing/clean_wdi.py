from sqlalchemy import text

def clean_wdi(engine):
    print("Cleaning WDI data")
    sql = """\
    DROP VIEW IF EXISTS wdicountry_cleaned;

    CREATE VIEW wdicountry_cleaned AS
    SELECT "Country Code" AS "Code",
           "Short Name" AS "Entity",
           "Currency Unit",
           "Region",
           "Income Group",
           "National accounts base year",
           "National accounts reference year",
           RIGHT("SNA price valuation", 5),
           "Lending category",
           RIGHT(LEFT("System of National Accounts", 21), 4)AS "System of National Accounts Methodology (year)",
           "System of trade",
           "Government Accounting concept",
           LEFT("Latest agricultural census", 4),
           "Latest industrial data",
           "Latest trade data"
    FROM wdicountry;
    """
    sql = text(sql)
    _ = engine.execute(sql)

    sql = """\
    DROP VIEW IF EXISTS wdicountry_series_cleaned;

    CREATE VIEW wdicountry_series_cleaned AS
    SELECT "CountryCode" AS "Code",
           "SeriesCode",
           "DESCRIPTION" AS "Description"
    FROM wdicountry_series;
    """
    sql = text(sql)
    _ = engine.execute(sql)

    sql = """\
    DROP VIEW IF EXISTS wdidata_pivot_cleaned;

    CREATE VIEW wdidata_pivot_cleaned AS
    SELECT "Country Code" AS "Code",
           "Indicator Code" AS "SeriesCode",
           variable AS "Year",
           ROUND(value) AS "Measure"
    FROM wdidata_pivot;
    """
    sql = text(sql)
    _ = engine.execute(sql)

    sql = """\
    DROP VIEW IF EXISTS wdifootnote_descriptions_cleaned;

    CREATE VIEW wdifootnote_descriptions_cleaned AS
    SELECT key AS "Footnote Id",
           value AS "Description"
    FROM wdifootnote_descriptions;
    """
    sql = text(sql)
    _ = engine.execute(sql)

    sql = """\
    DROP VIEW IF EXISTS wdifootnote_ids_cleaned;

    CREATE VIEW wdifootnote_ids_cleaned AS
    SELECT "CountryCode" AS "Code",
           "SeriesCode",
           "Year",
           "DESCRIPTION_ID" AS "Footnote Id"
    FROM wdifootnote_ids;
    """
    sql = text(sql)
    _ = engine.execute(sql)
    print("Finished cleaning WDI data")