import io

def df_to_sql(df, name, engine, if_exists='replace'):
    df.head(0).to_sql(name, engine, if_exists=if_exists, index=False)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    content = output.getvalue()
    cursor.copy_from(output, name, null="")
    connection.commit()