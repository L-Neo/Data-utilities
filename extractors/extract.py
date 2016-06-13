from connectors import sqldb
from sqlalchemy import text
import pandas as pd

class FromDB:
    """
    Runs queries against a database.
    
    Args:
        connector (instance): An instance of the connector class.
        db (string): The database credential in the .env file.
        
    Attributes:
        connector (object): An SQLAlchemy engine object.
        engine (object): The sqlalchemy engine object for running queries.
    """
    def __init__(self, connector):
        # load the database engine       
        self.engine = connector.engine
        self.data = pd.DataFrame()

    def RunQuery(self, sqlquery, to_csv=False, filename=''):
        """
        Runs a text query, returns data, and gives the option of saving it to a CSV.

        Args:
           sqlquery (string): Multi line string query.
           to_csv (boolean): If True, data is stored to CSV.

        Returns:
            data (dataframe): A pandas dataframe
            file (CSV): Writes a CSV with the specified filename
        """
        conn = self.engine.connect()
        query = text(sqlquery)
        data = pd.read_sql_query(query, conn)
        conn.close()

        if to_csv == True:
            if filename=='':
                raise ValueError('CSV filename cannot be blank.')
            else:
                data.to_csv(filename, encoding='utf-8')
                return data
        return data