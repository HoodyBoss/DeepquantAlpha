def get_tfex_morning_open():
    return '0945'


def get_tfex_morning_close():
    return '1230'


def get_tfex_afternoon_open():
    return '1415'


def get_tfex_afternoon_close():
    return '1655'


def get_set_morning_open():
    return '1000'


def get_set_morning_close():
    return '1230'


def get_set_afternoon_open():
    return '1430'


def get_set_afternoon_close():
    return '1630'


def is_morning_tfex_sess(datetime_obj):
    result = False
    open_time = int(get_tfex_morning_open())
    close_time = int(get_tfex_morning_close())
    bkk_time = int(datetime_obj.strftime('%H%M'))

    if close_time > bkk_time >= open_time:
        result = True

    return result


def is_afternoon_tfex_sess(datetime_obj):
    result = False
    open_time = int(get_tfex_afternoon_open())
    close_time = int(get_tfex_afternoon_close())
    bkk_time = int(datetime_obj.strftime('%H%M'))

    if close_time > bkk_time >= open_time:
        result = True

    return result


def is_morning_set_sess(datetime_obj):
    result = False
    open_time = int(get_set_morning_open())
    close_time = int(get_set_morning_close())
    bkk_time = int(datetime_obj.strftime('%H%M'))

    if close_time > bkk_time >= open_time:
        result = True

    return result


def is_afternoon_set_sess(datetime_obj):
    result = False
    open_time = int(get_set_afternoon_open())
    close_time = int(get_set_afternoon_close())
    bkk_time = int(datetime_obj.strftime('%H%M'))

    if close_time > bkk_time >= open_time:
        result = True

    return result


def is_not_weekend(datetime_obj):
    result = True
    bkk_dayofweek = datetime_obj.strftime('%A')

    if bkk_dayofweek == 'Sunday' or bkk_dayofweek == 'Saturday':
        result = False

    return result
