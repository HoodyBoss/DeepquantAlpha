import pandas as pd

import deepquant.common.http_util as http_util
import deepquant.data.db_gateway as db_gateway


def backfill_price(db_host, db_port, market, broker_id, strategy_name, symbol_name, timeframe, price_file):
    """
    # Example:
    strategy_name = 'gw_tfex1'
    db_host = 'localhost'  # ถ้าจะ backfill เข้า database บน cloud ให้แก้ host_name ให้เป็น ip address ของ droplet บน cloud
    db_port = '4442'
    market = 'TFEX'
    broker_id = ''  # สำหรับตลาด TFEX และหุ้นไทย ไม่ต้องใส่ค่าอะไร ให้พิมพ์ ''
    symbol_name = 'S50'
    timeframe = 'M5'
    """
    try:
        df = pd.read_csv(price_file)
        print('columns = {}'.format(list(df.columns)))

        if 'time' in list(df.columns):
            if type(df['time'].iloc[0]) != str:
                df['date'] = (df['date'] * 1000000) + df['time']
                df['date'] = df['date'].astype(str)
            else:
                df['date'] = df['date'] + df['time']
            df = df.drop(columns=['time'])

        df.columns = df.columns.str.strip().str.upper().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
        if 'volume' not in df.columns and 'VOLUME' not in df.columns:
            df['volume'] = 0.0

        df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        df['volume'] = df['volume'].astype(float)

        if df.shape[0] > 40000:
            start_row = 40000
            end_row = df.shape[0] - 1
            json_payload = df.iloc[start_row:end_row, :].to_json(orient='records')
        else:
            json_payload = df.to_json(orient='records')

        """
        response = http_util.post(url, json_payload, header_dict=None)
        if response == True:
            print('Backfill price data successfully')
        else:
            print('Backfill price data failed')
        """
        result = db_gateway.backfill(db_host, db_port, market, broker_id, symbol_name, timeframe, json_payload)
        print(result)

    except Exception as e:
        print('Backfill price data failed: {}'.format(e))
        raise e


# ====================================================================================================
# Instruction: edit file path and file name before testing
price_file = '/Users/minimalist/GoogleDrive/Datasets/TFEX/S50_M5.csv'
# The following is for Windows (NOTE: edit path)
# price_file = 'C:\\DeepQuantProjects\\price_data\\FOREX\\XAUUSD_M5.csv'
# ====================================================================================================
strategy_name = 'gw_tfex1'
db_host = 'localhost'  # ถ้าจะ backfill เข้า database บน cloud ให้แก้ host_name ให้เป็น ip address ของ droplet บน cloud
db_port = '4442'
market = 'TFEX'
broker_id = ''  # สำหรับตลาด TFEX และหุ้นไทย ไม่ต้องใส่ค่าอะไร ให้พิมพ์ ''
symbol_name = 'S50'
timeframe = 'M5'
backfill_price(db_host, db_port, market, broker_id, strategy_name, symbol_name, timeframe, price_file)
