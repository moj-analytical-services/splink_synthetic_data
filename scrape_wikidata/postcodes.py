import requests
import logging
import json


import http.client as http_client
from urllib.parse import urlparse, urlunparse, urlencode


API_BASE_URL = "http://api.postcodes.io"
API_BASE_URL = "http://localhost:8000"


class Api(object):
    def __init__(self, debug_http=False, timeout=None, base_url=None):

        self._debug_http = debug_http
        self._timeout = timeout
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = API_BASE_URL

        if debug_http:
            http_client.HTTPConnection.debuglevel = 1
            logging.basicConfig()  # initialize logging
            logging.getLogger().setLevel(logging.DEBUG)
            requests_log = logging.getLogger("requests.packages.urllib3")
            requests_log.setLevel(logging.DEBUG)
            requests_log.propagate = True
        self._session = requests.Session()

    def get_bulk_reverse_geocode(self, payload_data):
        url = "/postcodes"
        response = self._make_request("POST", url, json=payload_data)
        data = self._parse_json_data(response.content.decode("utf-8"))
        return data

    def _make_request(self, http_method, url, data=None, json=None):
        """
        :param http_method: http method i.e. GET, POST, PUT etc
        :param url: api endpoint url
        :param data: dictionary of key/value params
        :param json: json data
        :return:
        """
        if not data:
            data = {}

        response = 0
        if http_method == "GET":
            url = self._build_url(url, extra_params=data)
            response = self._session.get(url, timeout=self._timeout)
        elif http_method == "POST":
            url = self._build_url(url)
            if data:
                response = self._session.post(url, data=data, timeout=self._timeout)
            elif json:
                response = self._session.post(url, json=json, timeout=self._timeout)
        return response

    def _build_url(self, url, path_elements=None, extra_params=None):
        url = self.base_url + url
        (scheme, netloc, path, params, query, fragment) = urlparse(url)

        # Add extra_parameters to the query
        if extra_params and len(extra_params) > 0:
            extra_query = self._encode_parameters(extra_params)
            if query:
                query += "&" + extra_query
            else:
                query = extra_query
        return urlunparse((scheme, netloc, path, params, query, fragment))

    @staticmethod
    def _encode_parameters(parameters):
        """
        Return url encoded string in 'key=value&key=value' format
        """
        if not parameters or not isinstance(parameters, dict):
            return None
        return urlencode(parameters)

    @staticmethod
    def _parse_json_data(json_data):
        try:
            data = json.loads(json_data)
        except ValueError:
            data = json.loads('{"Error": "Unknown error while parsing response"}')
        return data


import requests
import json


def get_random_postcode(x):
    r = requests.get("http://127.0.0.1:8000/random/postcodes")
    r = json.loads(r.text)["result"]

    random_postcode = {}
    random_postcode["postcode"] = r["postcode"]
    random_postcode["lat"] = r["latitude"]
    random_postcode["lng"] = r["longitude"]
    return random_postcode
