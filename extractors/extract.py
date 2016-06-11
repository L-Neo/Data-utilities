from sqlalchemy import text
import pandas as pd

class ExtractFromDB:
    """
    Runs queries against a database.
    
    Args:
        connector (string): The name of the connector class.
        db (string): The database credential in the .env file.
        
    Attributes:
        connector (object): An instance of the connector class.
        engine (object): The sqlalchemy engine object for running queries.
    """
    def __init__(self, connector, db='DW'):
        # load the database engine       
        self.engine = getattr(sqldb,connector)(db).engine

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