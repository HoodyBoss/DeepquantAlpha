
import deepquant.data.influxdb_util as influxdb_util

from structlog import wrap_logger, PrintLogger
from structlog.processors import JSONRenderer
import datetime
import pytz
utc_tz = pytz.timezone('UTC')
logger = wrap_logger(PrintLogger(), processors=[JSONRenderer()])
log = logger.bind(time="NONE"
                    , level="INFO", events="NONE"
                    , market="NONE", broker_id="NONE", symbol_name="NONE", timeframe="NONE"
                    , details="NONE")

def insert_price(db_host, db_port, market, broker_id, symbol_name, timeframe, price_json):
    try:
        influxdb_util.insert_price(db_host, db_port, market, broker_id, symbol_name, timeframe, price_json)
    except Exception as e:
        dt = datetime.datetime.now().astimezone(utc_tz)
        log.error(time="{}".format(dt.isoformat())
                            , level="ERROR", events="Insert price"
                            , market=market, broker_id=broker_id, symbol_name=symbol_name, timeframe=timeframe
                            , details="Insert price error: {}".format(e))

def backfill(db_host, db_port, market, broker_id, symbol_name, timeframe, price_json):
    result = None
    try:
        result = influxdb_util.backfill(db_host, db_port, market, broker_id, symbol_name, timeframe, price_json)
    except Exception as e:
        dt = datetime.datetime.now().astimezone(utc_tz)
        log.error(time="{}".format(dt.isoformat())
                  , level="ERROR", events="Backfill price data"
                  , market=market, broker_id=broker_id, symbol_name=symbol_name, timeframe=timeframe
                  , details="Backfill price data error: {}".format(e))
    return result

def query_price(db_host, db_port, market, broker_id, symbol_name, timeframe \
                , upper_col_name=False, limit_rows=None):
    """
    Query price. Return type is pandas DataFrame.
    'upper_col_name': True, column name in DataFrame will use upper case.
    'limit_rows': a number of rows to be returned, if use 0 will return all rows.
    """
    result = None
    try:
        result = influxdb_util.query_price(db_host, db_port, market, broker_id, symbol_name, timeframe \
                    , upper_col_name=upper_col_name, limit_rows=limit_rows)
    except Exception as e:
        dt = datetime.datetime.now().astimezone(utc_tz)
        log.error(time="{}".format(dt.isoformat())
                  , level="ERROR", events="Query price"
                  , market=market, broker_id=broker_id, symbol_name=symbol_name, timeframe=timeframe
                  , details='Query price error: {}'.format(e))
    return result

def query(db_host, db_port, market, query_stmt, bind_params=None):
    """
    Query data. Return InfluxDB points.
    """
    try:
        result = influxdb_util.query(db_host, db_port, market, query_stmt, bind_params)
    except Exception as e:
        dt = datetime.datetime.now().astimezone(utc_tz)
        log.error(time="{}".format(dt.isoformat())
                  , level="ERROR", events="Query data"
                  , market=market
                  , details='Query error: {}'.format(e))
    return result

def write_time_series_data(db_host, db_port, market, data, time_precision=None):
    """
    Insert/update data.
    """
    result = False
    try:
        result = influxdb_util.write_points(db_host, db_port, market, data, time_precision=time_precision)
    except Exception as e:
        dt = datetime.datetime.now().astimezone(utc_tz)
        log.error(time="{}".format(dt.isoformat())
                  , level="ERROR", events="Write data point(s)"
                  , market=market
                  , details='Write data point(s) error: {}'.format(e))
    return result
