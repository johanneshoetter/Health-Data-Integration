import pandas as pd
from collections import defaultdict

def denormalize_ghdx(engine):
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
        'secondhand_smoke_deaths_by_age'
    ]

    select_map = defaultdict(list)
    for table in table_names:
        header_df = pd.read_sql_query("SELECT * FROM {} LIMIT 1".format(table), con=engine) # get first row to read column names
        select_map[table].extend(header_df.columns[3:])

    alias_map = {}
    alphabet = list('abcdefghijklmnoprstuvwxyz')
    for table in select_map.keys():
        alias_map[table] = alphabet.pop()

    sql_select = "SELECT z.[Code],\n\tz.[Year],"
    sql_join = ""
    last_alias = None  # not last alias that exist, but last alias that has been used
    for idx, (table, columns) in enumerate(select_map.items()):
        alias = alias_map[table]

        # build the select part
        sql_select += ', '.join(["\n\t{}.[{}]".format(alias, column) for column in columns]) + ','

        # build the from/join part
        if last_alias:  # INNER JOIN
            sql_join += "\nOUTER JOIN {table} {alias} \
            \n\tON {alias}.[Code] = {last_alias}.[Code] AND {alias}.[Year] = {last_alias}.[Year]".format(table=table, \
                                                                                                         alias=alias, \
                                                                                                         last_alias=last_alias)
        else:  # FROM
            sql_join = "\nFROM {} {}".format(table, alias)
        last_alias = alias
    sql = sql_select[:-1] + sql_join  # [:-1] to get rid of last commata

    print(sql)
    #df = pd.read_sql_query(sql, con=engine)
    #print(df.shape)