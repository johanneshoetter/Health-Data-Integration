import pandas as pd
import numpy as np
from collections import defaultdict
from sqlalchemy import text

from code.utils.dataframe_utils import df_to_sql


def denormalize_ghdx(engine):

    print("Beginning denormalization of ghdx data")
    engine.execute('DROP TABLE IF EXISTS {}'.format('ghdx_measures'))
    table_names = [
        'average_price_of_a_pack_of_cigarettes',
        'comparing_the_share_of_men_and_women_who_are_smoking',
        'consumption_per_smoker_per_day',
        'consumption_per_smoker_per_day_bounds',
        'daily_smoking_prevalence_bounds',
        'enforcement_of_bans_on_tobacco_advertising',
        'number_of_deaths_from_secondhand_smoke',
        'number_of_deaths_from_tobacco_smoking',
        'number_of_total_daily_smokers',
        'sales_of_cigarettes_per_adult_per_day',
        'secondhand_smoke_deaths_by_age',
        #'share_of_adults_who_are_smoking_by_level_of_prosperity',
        'share_of_adults_who_smoke',
        'share_of_cancer_deaths_attributed_to_tobacco',
        'share_of_tobacco_retail_price_that_is_tax',
        'smoking_and_secondhand_deaths',
        #'smoking_deaths_by_age',
        'support_to_help_to_quit_tobacco_use',
        'taxes_as_share_of_cigarette_price'
    ]

    sql = text('\nUNION\n'.join(['SELECT DISTINCT "Code", "Year" FROM {}'.format(table) for table in table_names]))
    code_year_combinations_df = pd.read_sql_query(sql, con=engine)

    select_map = defaultdict(list)
    for table in table_names:
        header_df = pd.read_sql_query("SELECT * FROM {} LIMIT 1".format(table), con=engine) # get first row to read column names
        select_map[table].extend(header_df.columns[3:])

    alias_map = {}
    alphabet = list('abcdefghijklmnoprstuvwxyz')
    for table in select_map.keys():
        alias_map[table] = alphabet.pop()

    sql_select = ''
    sql_join = ''
    sql_condition = ''
    last_alias = None  # not last alias that exist, but last alias that has been used
    for idx, (table, columns) in enumerate(select_map.items()):
        alias = alias_map[table]

        # build the select part
        sql_select += ', '.join(['\n\t{}."{}"'.format(alias, column) for column in columns]) + ','

        # build the from/join part
        if last_alias:  # OUTER JOIN
            sql_join += '\nFULL OUTER JOIN {table} {alias}\n\tON ' \
                        '{alias}."Code" = {last_alias}."Code" ' \
                        'AND {alias}."Year" = {last_alias}."Year"'.format(table=table, \
                                 alias=alias, \
                                 last_alias=last_alias)
            sql_condition += '\n\tOR ({alias}."Code" = \'<CODE>\' AND {alias}."Year" = \'<YEAR>\')'.format(alias=alias, last_alias=last_alias)
        else:  # FROM
            sql_join = "\nFROM {} {}".format(table, alias)
            sql_condition += '\nWHERE ({alias}."Code" = \'<CODE>\' AND {alias}."Year" = \'<YEAR>\')'.format(alias=alias, last_alias=last_alias)
        last_alias = alias
    sql_template = sql_select[:-1] + sql_join + sql_condition  # [:-1] to get rid of last commata

    # melting the rows together
    for code, year in code_year_combinations_df.values:
        sql = text('SELECT \'{code}\' "Code",\n\t \'{year}\'"Year",'.format(code=code, year=year) +
                   sql_template.replace('<CODE>', str(code)).replace('<YEAR>', str(year)))
        df = pd.read_sql_query(sql, con=engine)
        row = {}
        for col in df.columns:
            try:
                row[col] = np.max(df[col])  # there is only one row with a value, so max can be applied
            except:
                row[col] = np.nan
        df = pd.DataFrame(data=[row], columns=df.columns)
        df_to_sql(df, 'ghdx_measures', engine, if_exists='append')

    print("Finished denormalization of ghdx data")

    return table_names # might be needed for cleaning afterwards