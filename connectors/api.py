import json
import os
from dotenv import load_dotenv, find_dotenv
from urllib import request, parse
from base64 import b64encode

class Mixpanel:
    """
    Usese the Mixpanel API to export raw data. Adapted for Python3.5 from the Mixpanel python module.
    
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
        load_dotenv(find_dotenv(usecwd=True))
        self.api_secret = os.environ[projectname.upper() + '_MIXPANEL_SECRET']
        
    def request(self, methods, params, http_method='GET', format = 'json'):
        """
        Creates the request URL and sends gets the data
        
        Args:
            Methods (list): List of resource names for constructing the request URL. 
                            e.g. ['events','properties'] gives https://mixpanel.com/api/2.0/events/properties/values
            params (dictionary): A dictionary with the extra parameters associated with the API method.
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
        data = response.read().decode()
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