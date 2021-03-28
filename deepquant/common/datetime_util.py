import datetime
import pytz


def utcnow():
    utc_tz = pytz.timezone('UTC')
    dt = datetime.datetime.now().astimezone(utc_tz)
    return dt


def bangkok_now():
    bkk_tz = pytz.timezone('Asia/Bangkok')
    dt = datetime.datetime.now().astimezone(bkk_tz)
    return dt


def local_now():
    return datetime.datetime.now()


def localize_utc(datetime_str, datetime_format):
    #Example d = datetime.datetime.strptime('01/12/2011 16:43:45', '%d/%m/%Y %H:%M:%S')
    d = datetime.datetime.strptime(datetime_str, datetime_format)
    utc_tz = pytz.timezone('UTC')
    dt = utc_tz.localize(d)
    return dt


def localize_bangkok(datetime_str, datetime_format):
    #Example d = datetime.datetime.strptime('01/12/2011 16:43:45', '%d/%m/%Y %H:%M:%S')
    d = datetime.datetime.strptime(datetime_str, datetime_format)
    bkk_tz = pytz.timezone('Asia/Bangkok')
    dt = bkk_tz.localize(d)
    return dt


def days_diff(min_datetime, max_datetime, format):
    d1 = datetime.datetime.strptime(min_datetime, format)
    d2 = datetime.datetime.strptime(max_datetime, format)
    diff = (d2 - d1).total_seconds() / (60.0 * 60.0 * 24.0)
    return diff

def minutes_diff(min_datetime, max_datetime, format):
    d1 = datetime.datetime.strptime(min_datetime, format)
    d2 = datetime.datetime.strptime(max_datetime, format)
    diff = (d2 - d1).total_seconds() / 60.0
    return diff
