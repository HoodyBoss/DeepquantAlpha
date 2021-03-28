import requests


def post(url, request_param, header_dict=None, **kwargs):
    """
    Send HTTP POST request
    :param url: URL of destination
    :param header_dict: HTTP headers info in dictionary type
    :param request_param: HTTP request parameter in dictionary type
    :param kwargs['require_response'] if equals True will return response, False or None will return boolean
    :return: response of boolean, True = success, False = fail
    """

    result = False
    if header_dict != None and len(header_dict) > 0:
        response = requests.post(url, data=request_param, headers=header_dict)
    else:
        response = requests.post(url, data=request_param)

    if response.status_code == requests.codes.ok :
        result = True

    if 'require_response' in kwargs and kwargs['require_response'] == True:
        result = response

    return result


def post_json(url, json_payload, header_dict=None, **kwargs):
    """
    Send HTTP POST request with JSON data as request message
    :param url: URL of destination
    :param header_dict: HTTP headers info in dictionary type
    :param json_payload: JSON data format
    :param kwargs['require_response'] if equals True will return response, False or None will return boolean
    :return: response of boolean, True = success, False = fail
    """

    result = False
    if header_dict != None and len(header_dict) > 0:
        response = requests.post(url, json=json_payload, headers=header_dict)
    else:
        response = requests.post(url, json=json_payload)

    if response.status_code == requests.codes.ok:
        result = True

    if 'require_response' in kwargs and kwargs['require_response'] == True:
        result = response

    return result


def get(url, header_dict=None):
    """
    Send HTTP GET request and return response data
    :param url: URL of destination
    :param header_dict: HTTP headers info in dictionary type
    :return: response object created by requests library
    """
    if header_dict != None and len(header_dict) > 0:
        response = requests.get(url, headers=header_dict)
    else:
        response = requests.get(url)
    return response


def put(url, request_param, header_dict=None, **kwargs):
    """
    Send HTTP PUT request
    :param url: URL of destination
    :param header_dict: HTTP headers info in dictionary type
    :param request_param: HTTP request parameter in dictionary type
    :param kwargs['require_response'] if equals True will return response, False or None will return boolean
    :return: response of boolean, True = success, False = fail
    """

    result = False
    if header_dict != None and len(header_dict) > 0:
        response = requests.put(url, data=request_param, headers=header_dict)
    else:
        response = requests.put(url, data=request_param)

    if response.status_code == requests.codes.ok :
        result = True

    if 'require_response' in kwargs and kwargs['require_response'] == True:
        result = response

    return result


def patch(url, request_param, header_dict=None, **kwargs):
    """
    Send HTTP PATCH request
    :param url: URL of destination
    :param header_dict: HTTP headers info in dictionary type
    :param request_param: HTTP request parameter in dictionary type
    :param kwargs['require_response'] if equals True will return response, False or None will return boolean
    :return: response of boolean, True = success, False = fail
    """

    result = False
    if header_dict != None and len(header_dict) > 0:
        response = requests.patch(url, data=request_param, headers=header_dict)
    else:
        response = requests.patch(url, data=request_param)

    if response.status_code == requests.codes.ok :
        result = True

    if 'require_response' in kwargs and kwargs['require_response'] == True:
        result = response

    return result
