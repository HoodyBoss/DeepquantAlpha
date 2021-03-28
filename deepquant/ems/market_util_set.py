import requests
import datetime


def load_holidays():
    """
    Returns list of holidays. String format of each holiday is YYYYmmdd, for example '20200501'
    """
    holidays = []

    set_holidays_url = 'https://www.set.or.th/set/holiday.do'
    months = {'มกราคม': '01', 'กุมภาพันธ์': '02', 'มีนาคม': '03', 'เมษายน': '04', 'พฤษภาคม': '05', 'มิถุนายน': '06'
        , 'กรกฎาคม': '07', 'สิงหาคม': '08', 'กันยายน': '09', 'ตุลาคม': '10', 'พฤศจิกายน': '11', 'ธันวาคม': '12'}

    # Get year number in string
    year = datetime.datetime.now().strftime('%Y')

    # Load raw holidays via HTTP GET method
    response = requests.get(set_holidays_url, stream=True)

    i = 0
    prev_line = ''
    for line_byte in response.iter_lines():
        line = line_byte.decode("utf-8")
        if line and (' มกราคม</td>' in line or ' กุมภาพันธ์</td>' in line or ' มีนาคม</td>' in line \
                     or ' เมษายน</td>' in line or ' พฤษภาคม</td>' in line or ' มิถุนายน</td>' in line \
                     or ' กรกฎาคม</td>' in line or ' สิงหาคม</td>' in line or ' กันยายน</td>' in line \
                     or ' ตุลาคม</td>' in line or ' พฤศจิกายน</td>' in line or ' ธันวาคม</td>' in line) \
                and i > 0 \
                and ('>วันจันทร์</td>' in prev_line or '>วันอังคาร</td>' in prev_line or '>วันพุธ</td>' in prev_line \
                     or '>วันพฤหัสบดี</td>' in prev_line or '>วันศุกร์</td>' in prev_line):
            # Parse holiday and re-format
            holiday_temp = line.split('nowrap;">')[1].replace('</td>', '')
            items = holiday_temp.split(' ')
            day = items[0] if len(items[0]) == 2 else '0' + items[0]
            month = months[items[1]]
            holiday = '{}{}{}'.format(year, month, day)
            # Add to list of holidays
            holidays.append(holiday)

        i = i + 1
        prev_line = line
    return holidays
