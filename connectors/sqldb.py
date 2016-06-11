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
        engine (object): Postgres connection engine.
    """
    
    def __init__(self, db='DW'):
        # load the environment variables from .env
        load_dotenv(find_dotenv(usecwd=True))
        
        # create the connection string
        conn_string = url.URL('postgres',
                              database = os.environ[db.upper() + '_NAME'],
                              username = os.environ[db.upper() + '_USER'],
                              password = os.environ[db.upper() + '_PW'],
                              host = os.environ[db.upper() + '_HOST'],
                              port = os.environ[db.upper() + '_PORT'])
        
        # create the engine with sqlalchemy
        engine = create_engine(conn_string)
        self.engine = engine
        
        # test the connection
        conn = engine.connect()
        conn.close()
        
    def ListSchemas(self):
        """
        Stores the list of schemas in the database as an attribute.
        """
        return inspect(self.engine).get_schema_names()
    
    def GetTables(self, schema):
        """
        Lists the tables in a schema.
        
        Args:
            schema (string): The schema name
            
        Returns:
            table (list): A list of the tables in the schema
        """
        # get a list of available schemas
        schemas = self.ListSchemas()
        
        # return the list of tables
        tables = inspect(self.engine).get_table_names(schema)
        if schema in schemas:
            return tables
        else:
            raise ValueError('Available schemas are: %s' % ', '.join(schemas))