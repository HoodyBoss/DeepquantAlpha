import datetime as dt
import deepquant.common.line_notify as line_notify


def print_error(robot_name, message, notify_token):
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = now + ': [' + robot_name + ']:\n' + message
    line_notify.send_notify(robot_name, notify_token, )
    print("{0}: [{1}]: {2}".format(now, robot_name, message))

def print_out(robot_name, message):
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("{0}: [{1}]: {2}".format(now, robot_name, message))

