import pandas as pd
import logging
import pytz


import deepquant.common.datetime_util as datetime_util

# create logger
logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)


class TradeDataFeedHandler :

    def extract_indi(self, robot_context, price_df):
        """
        Extract indicators from prices
        :param robot_context: a robot context
        :param price_df: a price dataframe
        :return: indicators dataframe contains only 1 lastest row
        """
        #=============================================================================================================
        # BEGIN: prepare price
        #=============================================================================================================
        # Get dataset (dataframe) from session
        session_dataset_df = robot_context.dataset_context.get_dataset()
        # Set price to new row dataset
        # Append to dataset in session
        session_dataset_df = session_dataset_df.append(price_df)
        session_dataset_df.index = pd.to_datetime(session_dataset_df.index, utc=True)
        try:
            session_dataset_df.index = session_dataset_df.index.tz_convert(price_df.index.tz)
        except:
            pass
        # =============================================================================================================
        # END: prepare price
        # =============================================================================================================

        return robot_context.model_util.extract_indi(session_dataset_df)

    def extract_indi2(self, robot_context, session_dataset_df, price_df):
        """
        Extract indicators from prices
        :param robot_context: a robot context
        :param price_df: a price dataframe
        :return: indicators dataframe contains only 1 lastest row
        """
        #=============================================================================================================
        # BEGIN: prepare price
        #=============================================================================================================
        # Get subset of dataset (date, open, high, low, close)
        price_df = session_dataset_df.iloc[:, 0:4]
        # A one row DataFrame contains latest indicators
        latest_indi_df = robot_context.model_util.extract_indi(price_df)
        # =============================================================================================================
        # END: prepare price
        # =============================================================================================================

        return latest_indi_df

    def extract_feature(self, robot_context, price_indi_df):
        """
        Extract fdatures from prices and indicators
        :param robot_context: a robot context
        :param price_indi_df: a dataframe contains prices and indicators
        :return: features dataframe contains only 1 lastest row
        """
        min_price = robot_context.config['min_price']
        max_price = robot_context.config['max_price']
        min_macd = robot_context.config['min_macd']
        max_macd = robot_context.config['max_macd']
        max_skip_row = robot_context.config['max_skip_row']
        max_backward_row = robot_context.config['max_backward_row']

        # extract features, the result is a dataframe containing only 1 latest row
        feature_df = robot_context.model_util.extract_feature(price_indi_df,
                                                      min_price, max_price, min_macd, max_macd,
                                                      max_skip_row, max_backward_row)
        return feature_df

    def extract_feature2(self, robot_context, session_dataset_df):
        """
        Extract fdatures from prices and indicators
        :param robot_context: a robot context
        :param price_indi_df: a dataframe contains prices and indicators
        :return: features dataframe contains only 1 lastest row
        """
        total_indi_columns = len(robot_context.get_indi_columns())
        first_indi_column = 5 # previous column is close price
        last_indi_column = 4 + total_indi_columns

        price_indi_df = session_dataset_df.iloc[:, first_indi_column:last_indi_column]

        # extract features, the result is a dataframe containing only 1 latest row
        latest_feature_df = robot_context.model_util.extract_feature(price_indi_df)
        return latest_feature_df

    def prepare_input(self, robot_context, new_price_dict, need_indi=False, need_feature=False\
                      , indi_dict=None, feature_dict=None):
        """
        Prepare input data. การทำงานมีได้หลาย scenario เช่น
        1) Datasource หรือ client จะส่งมาเฉพาะราคาอย่างเดียว แล้วค่อยมา extract indicators และ extract features อีกที
        2) Datasource หรือ client ส่ง ราคา และ indicator แล้วค่อยมา extract features อีกที
        3) Datasource หรือ client ส่ง ราคา และ indicator แต่โมเดลไม่ได้ใช้ feature เช่น เป็น rule based model

        ดังนั้นจึงต้องระบุพารามิเตอร์มาว่าโมเดลใน trading robot นี้ต้องใช้ indicator / feature หรือไม่
        ส่วน indi_dict, feature_dict จะส่งมาด้วยหรือไม่ขึ้นกับการออกแบบ trading robot นั้นๆ
        :param robot_context: A robot context
        :param new_price_dict: new price dictionary
        :param need_indi: model needs to use indicators or not?
        :param need_feature: model needs to use features or not?
        :param indi_dict: indicator dictionary
        :param feature_dict: feature dictionary
        :return: price_indi_df, feature_df
        """

        # 1) Prepare new price
        """
        if "open" in new_price_dict:
            new_price_dict['o'] = new_price_dict['open']
            new_price_dict['h'] = new_price_dict['high']
            new_price_dict['l'] = new_price_dict['low']
            new_price_dict['c'] = new_price_dict['close']
            new_price_dict.pop('open', None)
            new_price_dict.pop('high', None)
            new_price_dict.pop('low', None)
            new_price_dict.pop('close', None)
        """

        session_dataset_df = robot_context.dataset_context.get_dataset()

        # Convert to DataFrame
        price_df = pd.DataFrame(new_price_dict, index=[0])
        price_df.set_index('date', inplace=True)
        logger.debug('%s - %s - [%s][%s]: %s' \
                    , datetime_util.bangkok_now(), __name__ \
                    , robot_context.config['robot_name'], 'DEBUG' \
                    , 'price_df.index before convert to datetime: '.format(price_df.index))
        price_df.index = pd.to_datetime(price_df.index)
        logger.debug('%s - %s - [%s][%s]: %s' \
                    , datetime_util.bangkok_now(), __name__ \
                    , robot_context.config['robot_name'], 'DEBUG' \
                    , 'price_df.index after converted to datetime: '.format(price_df.index))
        try:
            price_df.index = price_df.index.tz_localize(session_dataset_df.index.tz)
            logger.debug('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , robot_context.config['robot_name'], 'DEBUG' \
                        , 'price_df.index after converted timezone: '.format(price_df.index))
        except:
            pass

        # =============================================================================================================
        # 2) Extract indicators
        # =============================================================================================================
        indi_df = None
        if need_indi and indi_dict == None:
            indi_df = self.extract_indi(price_df)
        elif need_indi and indi_dict != None:
            indi_df = pd.DataFrame(data=[indi_dict], index=price_df.index)

        logger.debug('%s - %s - [%s][%s]: %s' \
                    , datetime_util.bangkok_now(), __name__ \
                    , robot_context.config['robot_name'], 'DEBUG' \
                    , 'indi_df: '.format(indi_df))

        # Append to dataset in session
        session_price_df = session_dataset_df.append(price_df)
        if "indi" in session_price_df.columns:
            session_price_df.drop('indi', axis=1, inplace=True)
        if "feat" in session_price_df.columns:
            session_price_df.drop('feat', axis=1, inplace=True)
        session_indi_df = robot_context.dataset_context.get_indicator()
        session_indi_df = session_indi_df.append(indi_df)
        session_indi_df.index = pd.to_datetime(session_indi_df.index, utc=True)

        try:
            session_indi_df.index = session_indi_df.index.tz_convert(session_price_df.index.tz)
        except:
            pass

        price_indi_df = pd.concat([session_price_df, session_indi_df], axis=1, sort=True)

        # =============================================================================================================
        # 3) Extract features
        # =============================================================================================================
        feature_df = None
        # Features for each bar processing has only 1 row
        if need_feature and feature_dict == None:
            feature_df = self.extract_feature(price_indi_df)
        elif need_feature and feature_dict != None:
            feature_df = pd.DataFrame(data=[feature_dict])

        # =============================================================================================================
        # 4) Update latest data (price, indicators, features, trade action analysis logs) in dataset in cache
        # =============================================================================================================
        session_indi_df_s = pd.Series(session_indi_df.to_dict('index'), name='indi')
        if feature_df is not None and len(feature_df):
            feat_s = pd.Series(feature_df.to_dict('index'), name='feat')
            data = pd.concat([session_price_df, session_indi_df_s, feat_s], axis=1, sort=True)
        else:
            data = pd.concat([session_price_df, session_indi_df_s], axis=1, sort=True)
        if session_indi_df.shape[0] >= robot_context.config['dataset_session_maxrow']:
            robot_context.dataset_context.drop_first_row()

        # Append last row of data to dataset in cache
        robot_context.dataset_context.append_row(data[-1:])
        logging.info("%s: %s", robot_context.config['robot_name'], "Append new row to dataset in cache successful")

        # Drop first row of dataframe (in memory)
        session_price_df = session_price_df.drop(session_price_df.index[[0]])
        session_indi_df = session_indi_df.drop(session_indi_df.index[[0]])

        # pricd_df and indi_df contains multiple rows
        # feature_df contains only one row
        return session_price_df, session_indi_df, feature_df

        #return None, None, None

    def prepare_input2(self, robot_context, new_price_dict, need_indi=False, need_feature=False\
                      , indi_dict=None, feature_dict=None):
        """
        Prepare input data. การทำงานมีได้หลาย scenario เช่น
        1) Datasource หรือ client จะส่งมาเฉพาะราคาอย่างเดียว แล้วค่อยมา extract indicators และ extract features อีกที
        2) Datasource หรือ client ส่ง ราคา และ indicator แล้วค่อยมา extract features อีกที
        3) Datasource หรือ client ส่ง ราคา และ indicator แต่โมเดลไม่ได้ใช้ feature เช่น เป็น rule based model

        ดังนั้นจึงต้องระบุพารามิเตอร์มาว่าโมเดลใน trading robot นี้ต้องใช้ indicator / feature หรือไม่
        ส่วน indi_dict, feature_dict จะส่งมาด้วยหรือไม่ขึ้นกับการออกแบบ trading robot นั้นๆ
        :param robot_context: A robot context
        :param new_price_dict: new price dictionary
        :param need_indi: model needs to use indicators or not?
        :param need_feature: model needs to use features or not?
        :param indi_dict: indicator dictionary
        :param feature_dict: feature dictionary
        :return: price_indi_df, feature_df
        """

        # 1) Prepare new price
        """
        if "open" in new_price_dict:
            new_price_dict['o'] = new_price_dict['open']
            new_price_dict['h'] = new_price_dict['high']
            new_price_dict['l'] = new_price_dict['low']
            new_price_dict['c'] = new_price_dict['close']
            new_price_dict.pop('open', None)
            new_price_dict.pop('high', None)
            new_price_dict.pop('low', None)
            new_price_dict.pop('close', None)
        """

        indi_columns = None
        feat_columns = None

        if need_indi and indi_dict != None:
            indi_columns = list(indi_dict.keys())
        if need_feature and feature_dict != None:
            feat_columns = list(feature_dict.keys())

        session_dataset_df = robot_context.dataset_context.get_dataset()

        # Convert to DataFrame
        price_df = pd.DataFrame(new_price_dict, index=[0])
        price_df.set_index('date', inplace=True)
        logger.debug('%s - %s - [%s][%s]: %s' \
                    , datetime_util.bangkok_now(), __name__ \
                    , robot_context.config['robot_name'], 'DEBUG' \
                    , 'price_df.index before convert to datetime: '.format(price_df.index))
        price_df.index = pd.to_datetime(price_df.index, utc=True)
        logger.debug('%s - %s - [%s][%s]: %s' \
                    , datetime_util.bangkok_now(), __name__ \
                    , robot_context.config['robot_name'], 'DEBUG' \
                    , 'price_df.index after converted to datetime: '.format(price_df.index))
        try:
            price_df.index = price_df.index.tz_localize(session_dataset_df.index.tz)
            logger.debug('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , robot_context.config['robot_name'], 'DEBUG' \
                        , 'price_df.index after converted timezone: '.format(price_df.index))
        except:
            pass

        # =============================================================================================================
        # 2) Extract indicators
        # =============================================================================================================
        if need_indi and indi_dict == None:
            for i in range(0, len(indi_columns)):
                price_df[indi_columns[i]] = 0.0

            if feat_columns != None:
                for i in range(0, len(feat_columns)):
                    price_df[feat_columns[i]] = 0.0

            # Append latest prices to existing dataset in session
            session_dataset_df = session_dataset_df.append(price_df)
            session_dataset_df.index = pd.to_datetime(session_dataset_df.index, utc=True)
            try:
                session_dataset_df.index = session_dataset_df.index.tz_convert(price_df.index.tz)
            except:
                pass

            # Extract indicators (calculate indicators from prices)
            latest_price_indi_df = self.extract_indi(session_dataset_df)
            dataset_lastrow = len(session_dataset_df) - 1
            for i in range(0, len(indi_columns)):
                session_dataset_df[indi_columns[i]][dataset_lastrow] = latest_price_indi_df[indi_columns[i]][0]

        elif need_indi and indi_dict != None:
            # Set indicator items to price DataFrame
            for i in range(0, len(indi_columns)):
                price_df[indi_columns[i]] = indi_dict[indi_columns[i]]

            if feat_columns != None:
                for i in range(0, len(feat_columns)):
                    price_df[feat_columns[i]] = 0.0

            # Append latest prices to existing dataset
            session_dataset_df = session_dataset_df.append(price_df)
            session_dataset_df.index = pd.to_datetime(session_dataset_df.index, utc=True)
            try:
                session_dataset_df.index = session_dataset_df.index.tz_convert(price_df.index.tz)
            except:
                pass

        logger.debug('%s - %s - [%s][%s]: %s' \
                    , datetime_util.bangkok_now(), __name__ \
                    , robot_context.config['robot_name'], 'DEBUG' \
                    , 'price_df after set indicators: '.format(price_df))

        # =============================================================================================================
        # 3) Extract features
        # =============================================================================================================
        # Features for each bar processing has only 1 row
        if need_feature and feature_dict == None:
            latest_feat_df = self.extract_feature2(session_dataset_df)
            for i in range(0, len(feat_columns)):
                session_dataset_df[feat_columns[i]][dataset_lastrow] = latest_feat_df[feat_columns[i]]

        elif need_feature and feature_dict != None:
            # Set feature items to existing dataset in session
            for i in range(0, len(feat_columns)):
                session_dataset_df[feat_columns[i]][dataset_lastrow] = feature_dict[feat_columns[i]]

        # =============================================================================================================
        # 4) Update latest data to dataset in cache
        # =============================================================================================================
        # Append last row of data to dataset in cache
        robot_context.dataset_context.set_dataset(session_dataset_df)
        logging.info("%s: %s", robot_context.config['robot_name'], "Set new row to dataset and update dataset in cache successful")

        # Drop first row of existing dataset in session
        session_dataset_df = session_dataset_df.drop(session_dataset_df.index[[0]])

        return session_dataset_df


class BacktestDataFeedHandler:

    def prepare_input(self, robot_context):
        return None, None

