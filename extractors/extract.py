from connectors import sqldb, api
from sqlalchemy import text
from datetime import datetime
import pandas as pd
import tempfile as tp
import re
import os
import json

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

    def RunQuery(self, sqlquery, to_csv=False, filename=None):
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
            if filename is None:
                raise ValueError('CSV filename cannot be blank.')
            else:
                data.to_csv(filename, encoding='utf-8')
                return data
        return data

class FromMixpanelRaw:
    """
    Exports raw Mixpanel for a given date range.

    Args:
        projectname (string): The name of the project.
        from_date (string): The start date (YYYY-MM-DD) for extracting data (inclusive)
        to_date (string): The end date (YYYY-MM-DD) for extracting data (inclusive).

    Attributes:
        response (http response): The response object returned by the server.

    """
    def __init__(self, projectname, from_date, to_date):
        # validate the datetime formats
        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()

        # save the response object
        self.response = api.Mixpanel(projectname).request(
            methods = ['export'],
            params = {'from_date':from_date, 'to_date': to_date})

    def RunAPI(self, events=0, to_csv=False, filename=None):
        """
        Get data from Mixpanel as a pandas dataframe.

        Args:
            events (int): The number of events to output. If not specified, returns all events.
            to_csv (boolean): Specify if a csv should be saved.
            filename (string): A string for the filename and location.

        Returns:
            result (dataframe): A dataframe with data on all events between the specified dates.
        """

        # write the response object to a temp file
        temp = tp.NamedTemporaryFile(prefix='tmp', suffix='.txt', delete=False)

        try:
            # write the data into the temp file
            with open(temp.name, 'w') as file:
                file.write(self.response)

            if int(events) > 0:
                # parse only the first n events from the json file
                result = self.ParseJsonRows(temp.name, events)
            else:
                # parse the whole json file
                result = self.ParseJsonFile(temp.name)

        finally:
            # remove the temp file
            os.remove(temp.name)

        # option to write data to csv
        if to_csv == True:
            if filename is None:
                raise ValueError('CSV filename cannot be blank.')
            else:
                result.to_csv(filename, encoding='utf-8')
                return result

        return result

    def ParseJsonRows(self, filename, events):
        """
        Parses a specified number of events from a JSON file into a dataframe.

        Args:
            filename (string): The filename containing the newline JSON.
            events: The
        """
        # create a df for storing the results
        result = pd.DataFrame()

        with open(filename, 'r') as file:
            for i in range(events):
                data = file.readline()

                # break if the end of file is reached
                if data == '':
                    break

                # create a dictionary for each event
                data = json.loads(data)
                df = {'event_name':data['event']}
                df.update(data['properties'])

                # convert the dictionary to a dataframe
                df = pd.DataFrame(df, index=[i])

                # merge the dataframes
                result = pd.concat([result, df])

        # remove $ from column names
        result = result.rename(columns=lambda x: re.sub('[$]','',x))

        # convert time column from epoch to utc
        result['time'] = pd.to_datetime(result.time, unit='s')

        return result

    def ParseJsonFile(self, filename):
        """
        Parses events from a JSON file into a dataframe.

        Args:
            filename (string): The filename containing the newline JSON.
        """
        # create a df for storing the results
        result = pd.DataFrame()

        with open(filename, 'r') as file:
            # iterate over the whole file
            for index, line in enumerate(file):
                data = json.loads(line)
                df = {'event_name':data['event']}
                df.update(data['properties'])

                # convert the dictionary to a dataframe
                df = pd.DataFrame(df, index=[index])

                # merge the dataframes
                result = pd.concat([result, df])

        # remove $ from column names
        result = result.rename(columns=lambda x: re.sub('[$]','',x))

        # convert time column from epoch to utc
        result['time'] = pd.to_datetime(result.time, unit='s')

        return result