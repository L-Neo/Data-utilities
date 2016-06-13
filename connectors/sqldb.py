import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import url
from dotenv import find_dotenv, load_dotenv

class Postgres:
    """
    A class for connecting with a postgres database and listing its schemas and tables.
    
    Args:
        db (string): The name used to identify the different databases in the .env file. e.g. heroku.
                     Defaults to 'DW', the datawarehouse.
        
    Attributes:
        engine (engine): SQLAlchemy connection engine.
        schemas (list): A list of all the schemas in the database.
    """
    
    def __init__(self, db='DW'):
        # load the environment variables from .env
        load_dotenv(find_dotenv(usecwd=True))
        
        # create the connection string
        conn_string = url.URL('postgresql',
                              database = os.environ[db.upper() + '_NAME'],
                              username = os.environ[db.upper() + '_USER'],
                              password = os.environ[db.upper() + '_PW'],
                              host = os.environ[db.upper() + '_HOST'],
                              port = os.environ[db.upper() + '_PORT'])
        
        # create the engine with sqlalchemy
        engine = create_engine(conn_string)
        self.engine = engine
        
        # save a list of the schemas
        self.schemas = inspect(self.engine).get_schema_names()

        # test the connection
        conn = engine.connect()
        conn.close()
    
    def ListTables(self, schema):
        """
        Lists the tables in a schema.
        
        Args:
            schema (string): The schema name.
            
        Returns:
            table (list): A list of the tables in the schema.
        """
        
        # return the list of tables
        tables = inspect(self.engine).get_table_names(schema)
        if schema in self.schemas:
            return tables
        else:
            raise ValueError('Available schemas are: %s' % ', '.join(self.schemas))

    def ListViews(self, schema):
        """
        Lists the views in a schema.
        
        Args:
            schema (string): The schema name.

        Returns:
            views (list): A list of the views in the schema.
        """
        
        # return the list of views
        views = inspect(self.engine).get_view_names(schema)
        if schema in self.schemas:
            return views
        else:
            raise ValueError('Available schemas are: %s' % ', '.join(self.schemas))