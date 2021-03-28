import logging
import deepquant.common.dataset_context as ds_ctx
import deepquant.common.datetime_util as datetime_util
import deepquant.common.error as err

# create logger
logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)


class BaseRobotContext:

    def __init__(self, robot_config, model_util):
        # Prepare
        self.config = robot_config
        self.model_util = model_util

        """
        # Build dictionary of column names:
        # Note: โมเดลจะต้องมีการใช้ indicators เสมอ และจะเก็บใน database (MongoDB)
        # ซึ่ง indicators จะสร้างใน python หรือส่งมาจาก Amibroker หรือ MetaTrader ก็ได้
        # ส่วน features จะมีหรือไม่มีก็ได้
        if model_util.has_feature():
            col_dict = {'price_colnames':self.model_util.get_price_columns(),
                        'indi_colnames':self.model_util.get_indi_columns(),
                        'feature_colnames':self.model_util.get_feature_columns(),
                        'analysis_log_colnames':self.model_util.get_analysis_log_columns()}
        else:
            col_dict = {'price_colnames':self.model_util.get_price_columns(),
                        'indi_colnames':self.model_util.get_indi_columns(),
                        'analysis_log_colnames':self.model_util.get_analysis_log_columns()}
        """

        # Initialize asset dataset
        # Class DataSetContext is a wrapper class, encapsulating session using Redis
        logger.debug('%s - %s - [%s][%s]: %s' \
                    , datetime_util.bangkok_now(), __name__ \
                    , self.config['robot_name'], 'DEBUG' \
                    , "DB collection name is " + self.config['db_collection_name'])

        try:
            self.dataset_context = None
            self.dataset_context = ds_ctx.DataSetContext(robot_config['robot_name'], robot_config)
            df = self.dataset_context.get_dataset()
            logger.debug('%s - %s - [%s][%s]: %s' \
                    , datetime_util.bangkok_now(), __name__ \
                    , self.config['robot_name'], 'DEBUG' \
                         , "Dataset's head is " + str(df.head()))
        except Exception as e:
            raise err.DeepQuantError("Create DataSetContext error, db collection name: {}: {}".format(
                robot_config['db_collection_name'],
                e))

    # Concat price columns, indicator columns, feature columns, analysis log columns and then return list of all columns
    def concat_dataset_column(self):
        price_columns = self.model_util.get_price_columns().copy()
        indi_columns = self.model_util.get_indi_columns().copy()

        if self.model_util.has_feature():
            feature_columns = self.model_util.get_feature_columns().copy()

        analysis_log_columns = self.model_util.get_analysis_log_columns().copy()

        for i in range(len(indi_columns)):
            price_columns.append(indi_columns[i])

        if self.model_util.has_feature():
            for i in range(len(feature_columns)):
                price_columns.append(feature_columns[i])

        for i in range(len(analysis_log_columns)):
            price_columns.append(analysis_log_columns[i])

        return price_columns

    def get_asset_dataset(self):
        return self.dataset_context.get_dataset()

    def get_asset_dataset_last_row(self):
        return self.dataset_context.get_dataset_last_row()

    def get_features_last_row(self):
        if self.model_util.has_feature():
            features = self.dataset_context.get_feature_last_row()
            return features
        else:
            return None

    """
    def add_asset_dataset_row(self, row_df):
        # Append new row to asset dataset in session
        self.dataset_context.append_row(row_df)

        # Insert new row to database
        self.dataset_dao.insert(self.config.db_collection_name, row_df)
    """

    def set_features(self, features_dict):
        pass
