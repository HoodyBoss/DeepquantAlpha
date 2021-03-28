import pandas as pd
from importlib import import_module

import deepquant.data.db_gateway as db_gateway


class DataPipeline():

    def __init__(self, market, broker_id, conn_config=None):
        self.market = market
        self.broker_id = broker_id
        self.conn_config = conn_config

    def load_price_from_db(self, symbol_name, timeframe, upper_col_name=False, limit_rows=None):
        try:
            df = db_gateway.query_price(self.conn_config['db_host'], self.conn_config['db_port'] \
                                        , self.market, self.broker_id, symbol_name, timeframe \
                                        , upper_col_name=upper_col_name, limit_rows=limit_rows)
        except Exception as e:
            raise Exception('Load price from database error: {}'.format(e))
        return df

    def append_new_price(self, existing_price_df, price_dict):
        try:
            if 'volume' in price_dict.keys():
                price_dict['volume'] = price_dict['volume'].astype(int)
            elif 'VOLUME' in price_dict.keys():
                price_dict['VOLUME'] = price_dict['VOLUME'].astype(int)

            price_df = pd.DataFrame(data=[price_dict], columns=existing_price_df.columns)
            existing_price_df = existing_price_df.append(price_df)
            existing_price_df = existing_price_df.reset_index(drop=True)
        except Exception as e:
            raise Exception('Append new price error: {}'.format(e))
        return existing_price_df

    def build_features(self, build_feat_module, feature_file_path, price_file_path=None, price_df=None):
        try:
            builder = import_module(build_feat_module)
            feature_df = builder.build_features(feature_file_path, price_file_path=price_file_path, price_df=price_df)
        except Exception as e:
            raise Exception('Build features error: {}'.format(e))
        return feature_df

    def insert_price_to_db(self, symbol_name, timeframe, price_json):
        try:
            db_gateway.insert_price(self.conn_config['db_host'], self.conn_config['db_port'] \
                                    , self.market, self.broker_id, symbol_name, timeframe, price_json)
        except Exception as e:
            raise Exception('Insert price into database error: {}'.format(e))

