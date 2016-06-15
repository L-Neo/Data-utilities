import json
import os
import requests
from dotenv import load_dotenv, find_dotenv
from urllib import request, parse
from base64 import b64encode
from datetime import datetime, timedelta
from dateutil.parser import *
from simple_salesforce import Salesforce as sf

# load environment variables
load_dotenv(find_dotenv())

class Mixpanel:
    """
    Uses the Mixpanel API to export raw data. Adapted for Python3.5 from the Mixpanel python module.
    
    Args:
        projectname (string): The name of the Mixpanel project.

    Attributes:
        api_secret (string): the key of the mixpanel project.
    """
    
    ENDPOINT = 'https://mixpanel.com/api'
    DATA_ENDPOINT = 'https://data.mixpanel.com/api'
    VERSION = '2.0'
    
    def __init__(self, projectname):
        # load environment variables and store the api secret
        self.api_secret = os.environ[projectname.upper() + '_MIXPANEL_SECRET']
        
    def request(self, methods, params, http_method='GET', format = 'json'):
        """
        Creates the request URL and sends gets the data
        
        Args:
            Methods (list): List of resource names for constructing the request URL. 
                            e.g. ['events','properties'] gives https://mixpanel.com/api/2.0/events/properties/values
            params (dictionary): A dictionary with the extra parameters associated with the API method.

        Returns:
            data (newline delimited json): this should be stored in a file and parsed as it consists of lines of json.
        """
        
        # create the base URL string
        if methods[0] == 'export':
            request_url = '/'.join([self.DATA_ENDPOINT, str(self.VERSION)] + methods)
        else:
            request_url = '/'.join([self.ENDPOINT, str(self.VERSION)] + methods)
        if http_method == 'GET':
            data = None
            request_url = request_url + '/?' + self.unicode_urlencode(params)
        else:
            data = self.unicode_urlencode(params)
        
        # encode to binary before 
        encoded_secret = b64encode(os.environ['STOCKTAKE_DEV_MIXPANEL_SECRET'].encode('utf-8')).decode('utf-8')

        # create request headers
        headers = {'Authorization': 'Basic {secret}'.format(secret = encoded_secret)}

        # create the request and return results
        requester = request.Request(request_url, data, headers)
        response = request.urlopen(requester, timeout=120)
        
        # encode bytes into strings
        data = response.read().decode('utf-8')
        return data
        
    def unicode_urlencode(self, params):
        """
        Convert params to json format and correctly handle unicode url parameters.
        
        Args:
            params (dict): A dictionary of parameters for the API call.

        Returns:
            result (string): URL encoded string.
        """
        # check if parameters is a dictionary
        if isinstance(params, dict):
            params = list(params.items())
        
        # convert lists into json formatted lists
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)
        
        # converts any strings to unicode and encodes it as a url
        result = parse.urlencode(
            [(k, isinstance(v, str) and v.encode('utf-8') or v) for k, v in params]
        )
        return result

class Zendesk:
    """
    Implementation of the Zendesk  API to export raw data.

    Attributes:
        endpoint (string): The Zendesk parent API endpoint.
        email (string): Email account used which has an API token.
        token (string): API token.
    """
    def __init__(self):
        subdomain = os.environ['ZENDESK_SUBDOMAIN']

        self.endpoint = 'https://{subdomain}.zendesk.com/api/v2/'.format(subdomain=subdomain)
        self.email = os.environ['ZENDESK_EMAIL']
        self.token = os.environ['ZENDESK_TOKEN']

    def request_incremental(self, start_time=None):
        """
        Requests for data from the incremental exporter.

        Args:
            start_time (string): A string datetime which specfies the date to pull data from.
                                 start_time must be more than 5 minutes old.
                                 The default is 1 days worth of data.

        Returns:
            response (string): Request response string.
        """

        # parse start_time as a datetime
        start_time = parse(str(start_time)) if start_time is not None else datetime.today() - timedelta(days=1)

        # ensure that the start_time adheres to the rate limit
        time_limit = datetime.now() - timedelta(minutes=5)

        if start_time < time_limit:
            # calculate time offset from utc
            offset = (datetime.utcnow() - datetime.now()).total_seconds()

            # convert start time to utc epoch
            start_time = int((start_time - datetime(1970,1,1)).total_seconds() + offset)

        else:
            raise ValueError('Start time must be more than 5 minutes old.')

        # construct request url
        incremental_url = 'incremental/tickets.json/?start_time={datetime}'.format(datetime=start_time)
        request_url = self.endpoint + incremental_url

        # run the request
        auth = requests.auth.HTTPBasicAuth(self.email+'/token', self.token)
        response = requests.get(request_url, auth=auth)

        return response.text

class Salesforce:
    """
    Salesforce connector which uses simple_salesforce package.

    Attributes:
        session (login object): A Salesforce session is created.
    """
    def __init__(self):
        self.session = sf(
            username = os.environ['SF_USER'],
            password = os.environ['SF_PW'],
            security_token = os.environ['SF_TOKEN']
        )