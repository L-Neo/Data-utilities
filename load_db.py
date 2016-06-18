import os
import tempfile
from extractors import extract

def ToPostgres(df, schema, tablename, connector):
    """
    VERSION 1
    ISSUES
    1. Tried loading accounts table but it was alway missing 22 accounts.
    2. Did not manage to get odo to work.

    Saves a dataframe as a Postgres table. If there is a name clash, it overrides the existing table.

    Args:
        df (dataframe): The dataframe to be loaded as a table.
        schema (string): The name of the schema.
        tablename (string): The name of the table to be created / overwritten.
        connector (object): An instance of the connector class.
    """
    # writes 1 row to the database to get the types correct
    df[:1].to_sql(name=tablename, schema=schema, con=connector.engine, if_exists='replace')

    # create a raw connection
    conn = connector.engine.raw_connection()

    # open a temporary file for storing the data
    with conn.cursor() as cur, tempfile.NamedTemporaryFile(mode='w', suffix='.csv') as temp:

        # saves the remaining data to csv
        df[1:].to_csv(temp)

        # reads data from the csv and copies it to the table
        with open(temp.name, 'r') as file:
            query = 'copy %s.%s from stdin with (FORMAT CSV, HEADER TRUE)' % (schema, tablename)
            cur.copy_expert(query, file)
    conn.commit()