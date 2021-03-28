import logging
import deepquant.common.http_util as http_util


def send_notify(robot_name, line_token, message):
    result = False
    url = 'https://notify-api.line.me/api/notify'
    auth_header = 'Bearer ' + line_token
    header_dict = {'Authorization':auth_header}
    request_param = {'message':message}
    try:
        result = http_util.post(url, request_param, header_dict)
    except:
        logging.error("%s: %s", robot_name, 'Send LINE notification error')

    return result

#Test
#result = send_notify('S50_Nemo', 'xxxxx', 'Hello')
#print(result)
