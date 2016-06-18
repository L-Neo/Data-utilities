from data_utils.connectors import sqldb, api
from datetime import datetime
import pandas as pd
import tempfile as tp
import re
import os
import json

class FromMixpanelRaw:
    """
    Exports raw Mixpanel for a given date range.

    Args:
        projectname (string): The name of the project.
        from_date (string): The start date (YYYY-MM-DD) for extracting data (inclusive)
        to_date (string): The end date (YYYY-MM-DD) for extracting data (inclusive).

    Attributes:
        response (str): The response string returned by the server.

    """
    def __init__(self, projectname, from_date, to_date):
        # validate the datetime formats
        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()

        # save the response text
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
            events: The number of events to be parsed.
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

class FromZendeskTickets:
    """
    Runs the incremental exporter to export Zendesk tickets.
    Args:
        start_time (string): Datetime period to extract data for.

    Attributes:
        ticket_fields (tuple): Fields that should be extracted from the results.
    """
    def __init__(self, start_time=None):
        self.start_time = start_time

        # list of fields to parse
        self.ticket_fields = (
            'assignee_id','created_at','description','due_at','external_id','generated_timestamp',
            'id','organization_id','priority','subject','raw_subject','recipient','satisfaction_rating',
            'priority','requester_id','submitter_id','updated_at','status','tags'
        )

    def RunAPI(self, tickets=0, to_csv=False, filename=None):
        """
        gets 1 page of data from Zendesk as a pandas dataframe.
        """
        # retrieve the response
        response = json.loads(api.Zendesk().request_incremental(self.start_time))
        result = self.ParseTickets(response['tickets'], number=tickets)

        # option to write data to csv
        if to_csv == True:
            if filename is None:
                raise ValueError('CSV filename cannot be blank.')
            else:
                result.to_csv(filename, encoding='utf-8')
                return result

        return result

    def ParseTickets(self, tickets, number=0):
        """
        """
        # check if tickets is a list of tickets
        if isinstance(tickets, list) == False:
            raise ValueError('Pass in a list of tickets.')

        # check if number is an int
        number = int(number)

        result = pd.DataFrame()

        # iterate over the tickets
        for index, ticket in enumerate(tickets):
            if (number > 0 and index > number):
                break

            # select the initial fields from each ticket
            data = {k: ticket.get(k, None) for k in self.ticket_fields}

            # flatten the dictionary
            data = self.FlattenDict(data)

            # concatenate lists into strings
            for key, value in data.items():
                if isinstance(value, list):
                    data[key] = ','.join(value)

            # convert the dictionary into a dataframe and merge it with the result
            data = pd.DataFrame(data, index=[index])
            result = pd.concat((result, data))

        return result

    def FlattenDict(self, dictionary, parent_key='', result=None):
        """
        Recursively flattens nested dictionaries by concatenating the keys together.

        Args:
            dictionary (dict): Dictionary to be unnested.
            parent_key (string): The initial string which accumulates the keys.
            result (dict): The accumulator for the recursive function.

        Returns:
            result (dictionary): An unnested dictionary with concatenated keys.
        """
        # initialise the dictionary accumulator
        result = {} if result is None else result

        # loop over the keys and recursively concatenate them
        for key, value in dictionary.items():
            new_key = key if parent_key == '' else '|'.join([parent_key, key])

            if isinstance(value, dict):
                self.FlattenDict(value, parent_key=new_key, result=result)
            else:
                result[new_key] = value

        return result