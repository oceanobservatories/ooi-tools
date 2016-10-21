import requests


class XAssetServiceException(Exception):
    '''Indicates a request to uFrame's XAsset Service API failed.'''

    def __init__(self, status_code):
        message = 'XAsset Service request failed with status: {0}'.format(
                status_code)
        super(XAssetServiceException, self).__init__(message)
        self.status_code = status_code


class XAssetServiceAPI(object):
    '''Class used to communicate with uFrame's XAsset Service API.'''

    def __init__(self, xasset_url, session=None):
        self.__xasset_url = xasset_url
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
            raise XAssetServiceException(response.status_code)
        return response.json()

    @staticmethod
    def __post_json(url, json, session=None):
        if session is None:
            response = requests.post(url, json=json)
        else:
            response = session.post(url, json=json)
        if response.status_code != requests.codes.created:
            raise XAssetServiceException(response.status_code)
        return response.json()

    @staticmethod
    def __put_json(url, json, session=None):
        if session is None:
            response = requests.put(url, json=json)
        else:
            response = session.put(url, json=json)
        if response.status_code != requests.codes.ok:
            raise XAssetServiceException(response.status_code)
        return response.json()

    @staticmethod
    def __delete_json(url, session=None):
        if session is None:
            response = requests.delete(url)
        else:
            response = session.delete(url)
        if response.status_code != requests.codes.ok:
            raise XAssetServiceException(response.status_code)
        return response.json()

    ##################
    # XAsset Methods #
    ##################

    def get_records(self, subsite, node, sensor, deployment):
        url = '/'.join(
                       (self.__xasset_url, "events/deployment/inv",
                        subsite, node, sensor, str(deployment)))
        return self.__get_json(url, self.__session)
