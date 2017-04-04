import requests


class DOIServiceException(Exception):
    '''Indicates a request to uFrame's DOI Service API failed.'''

    def __init__(self, status_code):
        message = 'DOI Service request failed with status: {0}'.format(
                status_code)
        super(DOIServiceException, self).__init__(message)
        self.status_code = status_code


class DOIServiceAPI(object):
    '''Class used to communicate with uFrame's DOI Service API.'''

    def __init__(self, doi_url, session=None):
        self.__doi_url = doi_url
        self.__session = session

    ##################
    # Common Methods #
    ##################

    @staticmethod
    def __get_json(url, session=None):
        if session is None:
            response = requests.get(url)
        else:
            response = session.get(url)
        if response.status_code != requests.codes.ok:
            raise DOIServiceException(response.status_code)
        return response.json()

    @staticmethod
    def __post_json(url, json, session=None):
        if session is None:
            response = requests.post(url, json=json)
        else:
            response = session.post(url, json=json)
        if response.status_code not in [
                requests.codes.created, requests.codes.ok]:
            raise DOIServiceException(response.status_code)
        return response.json()

    @staticmethod
    def __put_json(url, json, session=None):
        if session is None:
            response = requests.put(url, json=json)
        else:
            response = session.put(url, json=json)
        if response.status_code != requests.codes.ok:
            raise DOIServiceException(response.status_code)
        return response.json()

    @staticmethod
    def __delete_json(url, session=None):
        if session is None:
            response = requests.delete(url)
        else:
            response = session.delete(url)
        if response.status_code != requests.codes.ok:
            raise DOIServiceException(response.status_code)
        return response.json()

    ######################
    # DOI Record Methods #
    ######################

    def test_connection(self):
        try:
            self.get_doi_record(1)
            return True
        except requests.exceptions.ConnectionError:
            return False

    def mark_parsed_data_sets_obsolete(self, subsite, node, sensor):
        url = '/'.join((self.__doi_url, "obsolete", subsite, node, sensor))
        return self.__get_json(url, self.__session)

    def get_doi_records(self):
        return self.__get_json(self.__doi_url, self.__session)

    def get_doi_record(self, id):
        url = '/'.join((self.__doi_url, str(id)))
        try:
            return self.__get_json(url, self.__session)
        except DOIServiceException, e:
            if e.status_code == requests.codes.not_found:
                return None
            raise e

    def create_doi_record(self, doi_record):
        url = '/'.join((self.__doi_url, "unique"))
        return self.__post_json(url, doi_record, self.__session)

    def delete_doi_record(self, id):
        url = '/'.join((self.__doi_url, str(id)))
        return self.__delete_json(url, self.__session)
