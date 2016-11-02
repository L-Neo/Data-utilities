import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import url
from dotenv import find_dotenv, load_dotenv
from sshtunnel import SSHTunnelForwarder

# load the environment variables
load_dotenv(find_dotenv())

class DB:
    """
    A class for connecting with a postgres database and listing its schemas and tables.

    Args:
        db_type (string): RDBMS type. e.g. Postgres.
        db (string): The name used to identify the different databases in the .env file. e.g. heroku.
        requires_tunnel (bool): The truth value for whether a ssh tunnel is required.

    Attributes:
        engine (engine): SQLAlchemy connection engine.
        sshtunnel (SSH tunnel forwarder): The SSH tunnel forwarder connection string.
    """

    def __init__(self, db_type, db, requires_tunnel=False):
        # create the connection string
        conn_string = url.URL(db_type,
                              database = os.environ[db.upper() + '_NAME'],
                              username = os.environ[db.upper() + '_USER'],
                              password = os.environ[db.upper() + '_PW'],
                              host = os.environ[db.upper() + '_HOST'],
                              port = os.environ[db.upper() + '_PORT'])

        if requires_tunnel:
            self.sshtunnel = SSHTunnelForwarder(
                os.environ['SSH_ADDRESS'],
                ssh_username=os.environ['SSH_USER'],
                ssh_pkey=os.environ['SSH_PKEY'],
                remote_bind_address=(os.environ['SSH_REMOTE_ADDRESS'], int(os.environ['SSH_REMOTE_PORT'])),
                local_bind_address=(os.environ['SSH_LOCAL_ADDRESS'], int(os.environ['SSH_LOCAL_PORT'])))

            # create the engine with sqlalchemy
            engine = create_engine(conn_string)
            self.engine = engine

        else:
            # create the engine with sqlalchemy
            engine = create_engine(conn_string)
            self.engine = engine

            # test the connection
            conn = engine.connect()
            conn.close()