from influxdb import DataFrameClient, InfluxDBClient
import json
import pandas as pd


def insert_price(db_host, db_port, market, broker_id, symbol_name, timeframe, price_json):
    try:
        data_dict = json.JSONDecoder().decode(price_json)

        df = pd.DataFrame.from_dict(data_dict, orient='columns')
        new_df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]
        new_df['datetime'] = pd.to_datetime(new_df['datetime'])
        new_df['datetime'] = new_df['datetime'].dt.tz_localize('UTC')
        new_df.set_index('datetime', inplace=True)

        new_df['open'] = new_df['open'].astype(float)
        new_df['high'] = new_df['high'].astype(float)
        new_df['low'] = new_df['low'].astype(float)
        new_df['close'] = new_df['close'].astype(float)

        # Treat field 'volume' data type as float !!! NOT integer
        new_df['volume'] = new_df['volume'].astype(float)

        dbname = 'price_db_{}'.format(market.lower())
        measurement_name = '{}_{}_{}'.format(broker_id.lower(), symbol_name.lower(), timeframe.lower())
        client = DataFrameClient(host=db_host, port=db_port)

        try:
            client.switch_database(dbname)
            client.write_points(new_df, measurement=measurement_name \
                                    , field_columns=['open', 'high', 'low', 'close', 'volume'], time_precision='s')
        except Exception as e:
            print(e)
            print('Database not found then create new database')
            try:
                client.create_database(dbname)
                client.switch_database(dbname)
                client.write_points(new_df, measurement=measurement_name \
                                , field_columns=['open', 'high', 'low', 'close', 'volume'], time_precision='s')
            except Exception as ex:
                raise Exception(ex)

    except Exception as e:
        raise Exception(e)

def backfill(db_host, db_port, market, broker_id, symbol_name, timeframe, price_json):
    """
    This function will decorate column names and datetime column format and data type before insert into database.
    Column names must be datetime, open, high, low, close, volume. And Must be lowercase.
    Datetime format is %Y%m%d%H%M%S, for example 20190501143000
    Datetime datatype is string

    Sample JSON data and format:
    data = '['
    data = data + '{"datetime":"20191107120000", "open":1480.5, "high":1481.2, "low":1480.1, "close":1480.9, "volume":0.0},'
    data = data + '{"datetime":"20191107120500", "open":1480.9, "high":1481.5, "low":1480.8, "close":1481.4, "volume":0.0},'
    data = data + '{"datetime":"20191107121000", "open":1481.4, "high":1482.4, "low":1481.3, "close":1482.1, "volume":0.0}'
    data = data + ']'
    """

    result = 'Backfill price data successful'
    try:
        insert_price(db_host, db_port, market, broker_id, symbol_name, timeframe, price_json)
    except Exception as e:
        raise Exception(e)
    return result


def query_price(db_host, db_port, market, broker_id, symbol_name, timeframe \
                , upper_col_name=False, limit_rows=None):
    """
    Query price data and return pandas DataFrame. The query uses order by descending.
    """
    result = None
    try:
        dbname = 'price_db_{}'.format(market.lower())
        measurement_name = '{}_{}_{}'.format(broker_id.lower(), symbol_name.lower(), timeframe.lower())
        client = DataFrameClient(host=db_host, port=db_port)
        client.switch_database(dbname)

        query_stmt = 'SELECT * FROM "{}" ORDER BY DESC'.format(measurement_name)
        if limit_rows is not None and limit_rows > 0:
            query_stmt = query_stmt + ' LIMIT {}'.format(limit_rows)
        result_dict = client.query(query_stmt)

        df = result_dict['{}_{}_{}'.format(broker_id.lower(), symbol_name.lower(), timeframe.lower())]
        df.index.name = 'datetime'
        df = df.reset_index()
        dt_format = '%Y%m%d%H%M%S'
        df['datetime'] = df['datetime'].dt.strftime(dt_format)
        #df['datetime'] = df['datetime'].astype(int)
        df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]

        if upper_col_name == True:
            df.columns = ['DATETIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']

        result = df
    except Exception as e:
        raise Exception(e)
    return result


def query(db_host, db_port, market, query_stmt, bind_params=None):
    """
    Query data using defined measurement_name and query_stmt, and return InfluxDB Point generator.
    Sample output usage
            points = influxdb_util.query(....)
            for point in points:
                print(point['close'])
    """
    try:
        dbname = 'price_db_{}'.format(market.lower())
        client = InfluxDBClient(host=db_host, port=db_port)
        client.switch_database(dbname)
    except Exception as e:
        raise Exception(e)
    if bind_params is not None:
        results = client.query(query_stmt, bind_params=bind_params)
    else:
        results = client.query(query_stmt)
    points = results.get_points()
    return points


def write_points(db_host, db_port, market, data, time_precision=None):
    try:
        dbname = 'price_db_{}'.format(market.lower())
        client = InfluxDBClient(host=db_host, port=db_port)
        client.switch_database(dbname)
        if time_precision is not None:
            result = client.write_points(data, time_precision=time_precision)
        else:
            result = client.write_points(data)
    except Exception as e:
        raise Exception(e)
    return result


#=================================================================================================================
# Test
# =================================================================================================================
def test_backfill():
    """
    data = '['
    data = data + '{"datetime":"20191107121500", "open":1480.5, "high":1481.2, "low":1480.1, "close":1480.9, "volume":0.0},'
    data = data + '{"datetime":"20191107122000", "open":1480.9, "high":1481.5, "low":1480.8, "close":1481.4, "volume":0.0},'
    data = data + '{"datetime":"20191107122500", "open":1481.4, "high":1482.4, "low":1481.3, "close":1482.1, "volume":0.0}'
    data = data + ']'
    """
    """
    file = '/Users/minimalist/Google Drive/DeepQuantProjects/BacktestPython/fx_barracuda_gold_ami5_price_2019_full.csv'
    df = pd.read_csv(file)

    master_col = ['DATETIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
    original_col = df.columns
    drop_col = list()
    for i in range(0, len(original_col)):
        if original_col[i] not in master_col:
            drop_col.append(original_col[i])
    df = df.drop(drop_col, axis=1)

    df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
    df['datetime'] = df['datetime'].astype(str)
    data = df.to_json(orient='records')

    result = backfill('localhost', 8086, 'FX', 'XM1', 'GOLD', 'M5', data)
    print(result)
    """
    result_df = query_price('localhost', 8086, 'FX', 'XM1', 'GOLD', 'M5', upper_col_name=True, limit_rows=300)
    print(result_df)

#test_backfill()
